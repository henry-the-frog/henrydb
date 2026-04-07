// query-compiler.js — Compile SQL execution plans into JavaScript functions
// Instead of interpreting the AST per row, generates optimized JS code
// that V8 can JIT-compile for 5-10x speedup.

/**
 * Compile a WHERE clause AST into a filter function.
 * 
 * @param {object} whereAst — the WHERE clause AST node
 * @param {Array<{name: string}>} schema — table schema (column definitions)
 * @returns {Function} — (values: any[]) => boolean
 */
export function compileFilter(whereAst, schema) {
  if (!whereAst) return () => true;
  
  // Build column name → index mapping
  const colMap = {};
  schema.forEach((col, i) => { colMap[col.name] = i; });
  
  const code = compileExpr(whereAst, colMap);
  
  try {
    // Generate a function that takes the values array
    return new Function('v', `"use strict"; return (${code});`);
  } catch (e) {
    // Fallback to interpreted mode if compilation fails
    return null;
  }
}

/**
 * Compile a projection (SELECT columns) into a function.
 * 
 * @param {Array} columns — the SELECT column list
 * @param {Array<{name: string}>} schema — table schema
 * @returns {Function} — (values: any[]) => object
 */
export function compileProjection(columns, schema) {
  if (!columns || columns.length === 0) return null;
  
  const colMap = {};
  schema.forEach((col, i) => { colMap[col.name] = i; });
  
  // Handle SELECT *
  if (columns.length === 1 && columns[0].type === 'star') {
    const body = schema.map((col, i) => `${JSON.stringify(col.name)}:v[${i}]`).join(',');
    return new Function('v', `"use strict"; return {${body}};`);
  }
  
  const parts = [];
  for (const col of columns) {
    if (col.type === 'column') {
      const idx = colMap[col.name];
      const alias = col.alias || col.name;
      if (idx !== undefined) {
        parts.push(`${JSON.stringify(alias)}:v[${idx}]`);
      }
    } else if (col.type === 'expression') {
      const exprCode = compileExpr(col.expr, colMap);
      const alias = col.alias || 'expr';
      parts.push(`${JSON.stringify(alias)}:(${exprCode})`);
    }
  }
  
  try {
    return new Function('v', `"use strict"; return {${parts.join(',')}};`);
  } catch (e) {
    return null;
  }
}

/**
 * Compile a full scan-filter-project pipeline into a single function.
 * 
 * @param {object} whereAst — WHERE clause
 * @param {Array} columns — SELECT columns
 * @param {Array<{name: string}>} schema — table schema
 * @param {object} [options] — { limit, offset, orderBy }
 * @returns {Function} — (heap: iterable) => object[]
 */
export function compileScanFilterProject(whereAst, columns, schema, options = {}) {
  const colMap = {};
  schema.forEach((col, i) => { colMap[col.name] = i; });
  
  const filterCode = whereAst ? compileExpr(whereAst, colMap) : 'true';
  
  // Build projection
  let projCode;
  if (!columns || (columns.length === 1 && columns[0].type === 'star')) {
    projCode = '{' + schema.map((col, i) => `${JSON.stringify(col.name)}:v[${i}]`).join(',') + '}';
  } else {
    const parts = [];
    for (const col of columns) {
      if (col.type === 'column') {
        const idx = colMap[col.name];
        const alias = col.alias || col.name;
        if (idx !== undefined) parts.push(`${JSON.stringify(alias)}:v[${idx}]`);
      } else if (col.type === 'expression') {
        const exprCode = compileExpr(col.expr, colMap);
        const alias = col.alias || 'expr';
        parts.push(`${JSON.stringify(alias)}:(${exprCode})`);
      }
    }
    projCode = '{' + parts.join(',') + '}';
  }
  
  const { limit, offset } = options;
  
  let body = `
    "use strict";
    const results = [];
    let skipped = 0;
    for (const entry of heap) {
      const v = entry.values;
      if (${filterCode}) {
        ${offset ? `if (skipped < ${offset}) { skipped++; continue; }` : ''}
        results.push(${projCode});
        ${limit ? `if (results.length >= ${limit}) break;` : ''}
      }
    }
    return results;
  `;
  
  try {
    return new Function('heap', body);
  } catch (e) {
    return null;
  }
}

// ===== Expression Compiler =====

function compileExpr(node, colMap) {
  if (!node) return 'true';
  
  switch (node.type) {
    case 'literal':
      return JSON.stringify(node.value);
    
    case 'column_ref': {
      const idx = colMap[node.name];
      if (idx === undefined) {
        // May be a qualified column (table.column) — try just the column name
        const parts = node.name.split('.');
        const name = parts[parts.length - 1];
        const qIdx = colMap[name];
        if (qIdx !== undefined) return `v[${qIdx}]`;
        return `v[${JSON.stringify(node.name)}]`; // fallback
      }
      return `v[${idx}]`;
    }
    
    case 'COMPARE': {
      const left = compileExpr(node.left, colMap);
      const right = compileExpr(node.right, colMap);
      const ops = { EQ: '===', NE: '!==', LT: '<', GT: '>', LE: '<=', GE: '>=' };
      const op = ops[node.op] || '===';
      return `(${left} ${op} ${right})`;
    }
    
    case 'AND':
      return `(${compileExpr(node.left, colMap)} && ${compileExpr(node.right, colMap)})`;
    
    case 'OR':
      return `(${compileExpr(node.left, colMap)} || ${compileExpr(node.right, colMap)})`;
    
    case 'NOT':
      return `(!(${compileExpr(node.expr || node.operand, colMap)}))`;
    
    case 'IS_NULL':
      return `(${compileExpr(node.expr || node.left, colMap)} == null)`;
    
    case 'IS_NOT_NULL':
      return `(${compileExpr(node.expr || node.left, colMap)} != null)`;
    
    case 'IN_LIST': {
      const left = compileExpr(node.left, colMap);
      const values = node.values.map(v => compileExpr(v, colMap));
      // Use a Set for large IN lists
      if (values.length > 10) {
        const setValues = values.join(',');
        return `(new Set([${setValues}]).has(${left}))`;
      }
      return `([${values.join(',')}].includes(${left}))`;
    }
    
    case 'BETWEEN': {
      const expr = compileExpr(node.expr || node.left, colMap);
      const low = compileExpr(node.low, colMap);
      const high = compileExpr(node.high, colMap);
      return `(${expr} >= ${low} && ${expr} <= ${high})`;
    }
    
    case 'LIKE': {
      const left = compileExpr(node.left, colMap);
      const pattern = node.right?.value || '';
      // Convert SQL LIKE to regex
      const regex = pattern
        .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        .replace(/%/g, '.*')
        .replace(/_/g, '.');
      return `(/^${regex}$/i.test(String(${left})))`;
    }
    
    case 'arith': {
      const left = compileExpr(node.left, colMap);
      const right = compileExpr(node.right, colMap);
      const ops = { '+': '+', '-': '-', '*': '*', '/': '/', '%': '%' };
      const op = ops[node.op] || '+';
      return `(${left} ${op} ${right})`;
    }
    
    case 'unary_minus':
      return `(-(${compileExpr(node.operand, colMap)}))`;
    
    case 'function_call': {
      // Common SQL functions
      const name = (node.name || '').toUpperCase();
      const args = (node.args || []).map(a => compileExpr(a, colMap));
      switch (name) {
        case 'ABS': return `Math.abs(${args[0]})`;
        case 'UPPER': return `String(${args[0]}).toUpperCase()`;
        case 'LOWER': return `String(${args[0]}).toLowerCase()`;
        case 'LENGTH': return `String(${args[0]}).length`;
        case 'COALESCE': return `(${args.map(a => `(${a})`).join(' ?? ')})`;
        case 'ROUND': return args.length > 1 ? 
          `(Math.round(${args[0]} * Math.pow(10, ${args[1]})) / Math.pow(10, ${args[1]}))` :
          `Math.round(${args[0]})`;
        default: return `null`; // Unknown function
      }
    }
    
    case 'CASE': {
      // CASE WHEN ... THEN ... ELSE ... END
      const whens = (node.whens || []).map(w => 
        `(${compileExpr(w.when, colMap)}) ? (${compileExpr(w.then, colMap)})`
      );
      const elseExpr = node.else ? compileExpr(node.else, colMap) : 'null';
      return `(${whens.join(' : ')} : ${elseExpr})`;
    }
    
    default:
      // Fallback for unknown node types
      return 'true';
  }
}

// Export for testing
export { compileExpr as _compileExpr };

// ===== Join Compiler =====

/**
 * Compile a hash join into a function.
 * 
 * @param {Array<{name: string}>} leftSchema — left table schema
 * @param {Array<{name: string}>} rightSchema — right table schema
 * @param {string} leftJoinCol — join column from left table
 * @param {string} rightJoinCol — join column from right table
 * @param {object} [whereAst] — additional WHERE clause
 * @returns {Function} — (leftHeap, rightHeap) => object[]
 */
export function compileHashJoin(leftSchema, rightSchema, leftJoinCol, rightJoinCol, whereAst) {
  const leftColMap = {};
  leftSchema.forEach((col, i) => { leftColMap[col.name] = i; });
  
  const rightColMap = {};
  rightSchema.forEach((col, i) => { rightColMap[col.name] = i; });
  
  const leftJoinIdx = leftColMap[leftJoinCol];
  const rightJoinIdx = rightColMap[rightJoinCol];
  
  if (leftJoinIdx === undefined || rightJoinIdx === undefined) return null;
  
  // Build the merged schema for the combined row
  const mergedColMap = {};
  leftSchema.forEach((col, i) => { mergedColMap[col.name] = i; });
  rightSchema.forEach((col, i) => { mergedColMap[col.name] = leftSchema.length + i; });
  
  const filterCode = whereAst ? compileExpr(whereAst, mergedColMap) : 'true';
  
  // Build projection for all columns
  const projParts = [];
  for (const col of leftSchema) {
    projParts.push(`${JSON.stringify(col.name)}:m[${leftColMap[col.name]}]`);
  }
  for (const col of rightSchema) {
    projParts.push(`${JSON.stringify(col.name)}:m[${leftSchema.length + rightColMap[col.name]}]`);
  }
  
  const body = `
    "use strict";
    // Build phase: index right table by join key
    const hashMap = new Map();
    for (const entry of rightHeap) {
      const key = entry.values[${rightJoinIdx}];
      if (!hashMap.has(key)) hashMap.set(key, []);
      hashMap.get(key).push(entry.values);
    }
    
    // Probe phase: scan left table and look up matches
    const results = [];
    for (const entry of leftHeap) {
      const lv = entry.values;
      const key = lv[${leftJoinIdx}];
      const matches = hashMap.get(key);
      if (!matches) continue;
      
      for (const rv of matches) {
        const m = [...lv, ...rv];
        if (${filterCode}) {
          results.push({${projParts.join(',')}});
        }
      }
    }
    return results;
  `;
  
  try {
    return new Function('leftHeap', 'rightHeap', body);
  } catch (e) {
    return null;
  }
}

/**
 * Compile a nested loop join into a function.
 */
export function compileNestedLoopJoin(leftSchema, rightSchema, joinCondition) {
  const mergedColMap = {};
  leftSchema.forEach((col, i) => { mergedColMap[col.name] = i; });
  rightSchema.forEach((col, i) => { mergedColMap[col.name] = leftSchema.length + i; });
  
  const filterCode = joinCondition ? compileExpr(joinCondition, mergedColMap) : 'true';
  
  const projParts = [];
  for (const col of leftSchema) {
    projParts.push(`${JSON.stringify(col.name)}:m[${mergedColMap[col.name]}]`);
  }
  for (const col of rightSchema) {
    projParts.push(`${JSON.stringify(col.name)}:m[${mergedColMap[col.name]}]`);
  }
  
  const body = `
    "use strict";
    const results = [];
    for (const le of leftHeap) {
      for (const re of rightHeap) {
        const m = [...le.values, ...re.values];
        if (${filterCode}) {
          results.push({${projParts.join(',')}});
        }
      }
    }
    return results;
  `;
  
  try {
    return new Function('leftHeap', 'rightHeap', body);
  } catch (e) {
    return null;
  }
}
