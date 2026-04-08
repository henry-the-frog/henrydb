// query-codegen.js — Batch compilation: generate one JavaScript function per query
// Instead of composing closures, generates JavaScript source code that V8 can
// optimize as a single compilation unit. This is the copy-and-patch approach
// adapted for JavaScript: template code with baked-in constants.

import { QueryPlanner } from './planner.js';

/**
 * QueryCodeGen — generates JavaScript functions from query plans.
 * 
 * Usage:
 *   const codegen = new QueryCodeGen(db);
 *   const { fn, source } = codegen.compile(ast);
 *   const rows = fn(db);  // Execute compiled query
 */
export class QueryCodeGen {
  constructor(database) {
    this.db = database;
    this.planner = new QueryPlanner(database);
    this._cache = new Map(); // SQL string → compiled function
    this.stats = { compiled: 0, cacheHits: 0, totalCompileMs: 0 };
  }

  /**
   * Compile a SELECT AST into an executable function.
   * Returns { fn: Function, source: string, plan: Object }
   */
  compile(ast) {
    const startMs = Date.now();

    // Plan the query
    const plan = this.planner.plan(ast);

    // Resolve table metadata
    const tables = this._resolveTableMeta(ast);
    if (!tables.main) return null;

    // Generate source code
    const source = this._generateSource(ast, plan, tables);
    if (!source) return null;

    // Compile the function
    try {
      const fn = new Function('db', source);
      this.stats.compiled++;
      this.stats.totalCompileMs += Date.now() - startMs;
      return { fn, source, plan };
    } catch (e) {
      console.error('Codegen compilation error:', e.message);
      console.error('Generated source:\n', source);
      return null;
    }
  }

  /**
   * Compile and execute a SELECT.
   * Returns { rows: [...] } or null (fall back to standard execution).
   */
  execute(ast) {
    const compiled = this.compile(ast);
    if (!compiled) return null;
    
    try {
      const rows = compiled.fn(this.db);
      return { rows };
    } catch (e) {
      console.error('Codegen execution error:', e.message);
      return null;
    }
  }

  /**
   * Resolve table metadata: column names, indices, heap references.
   */
  _resolveTableMeta(ast) {
    const result = {};
    const mainTableName = ast.from?.table;
    if (!mainTableName) return result;

    const mainTable = this.db.tables.get(mainTableName);
    if (!mainTable) return result;

    const mainAlias = ast.from.alias || mainTableName;
    const mainCols = mainTable.schema.map(c => c.name);
    result.main = { name: mainTableName, alias: mainAlias, columns: mainCols, colMap: {} };
    mainCols.forEach((c, i) => result.main.colMap[c] = i);

    // Join tables
    result.joins = [];
    for (const join of (ast.joins || [])) {
      const jTable = this.db.tables.get(join.table);
      if (!jTable) continue;
      const jAlias = join.alias || join.table;
      const jCols = jTable.schema.map(c => c.name);
      const jColMap = {};
      jCols.forEach((c, i) => jColMap[c] = i);
      result.joins.push({
        name: join.table,
        alias: jAlias,
        columns: jCols,
        colMap: jColMap,
        on: join.on,
        joinType: join.joinType || 'INNER',
      });
    }

    return result;
  }

  /**
   * Generate JavaScript source code for the query.
   */
  _generateSource(ast, plan, tables) {
    const lines = [];
    const indent = (n) => '  '.repeat(n);

    lines.push(`"use strict";`);
    lines.push(`const results = [];`);

    const mainName = tables.main.name;
    const mainAlias = tables.main.alias;
    const mainCols = tables.main.columns;

    // Emit table references
    lines.push(`const ${mainAlias}_table = db.tables.get(${JSON.stringify(mainName)});`);
    for (const jt of tables.joins) {
      lines.push(`const ${jt.alias}_table = db.tables.get(${JSON.stringify(jt.name)});`);
    }
    lines.push('');

    if (tables.joins.length === 0) {
      // Single-table query: simple scan + filter
      lines.push(...this._genSingleTableScan(ast, tables.main, plan));
    } else {
      // Multi-table: hash join(s)
      lines.push(...this._genJoinQuery(ast, tables, plan));
    }

    // Apply LIMIT
    if (ast.limit?.value) {
      // LIMIT is applied inline via break in the scan loop
    }

    lines.push(`return results;`);
    return lines.join('\n');
  }

  /**
   * Generate code for a single-table scan with optional filter and projection.
   */
  _genSingleTableScan(ast, tableMeta, plan) {
    const lines = [];
    const alias = tableMeta.alias;
    const cols = tableMeta.columns;
    const limit = ast.limit?.value;

    if (limit) {
      lines.push(`let _count = 0;`);
    }

    lines.push(`for (const {values} of ${alias}_table.heap.scan()) {`);
    
    if (limit) {
      lines.push(`  if (_count >= ${limit}) break;`);
    }

    // Filter
    if (ast.where) {
      const filterCode = this._genFilterExpr(ast.where, tableMeta.colMap, alias);
      if (filterCode) {
        lines.push(`  if (!(${filterCode})) continue;`);
      }
    }

    // Projection
    lines.push(`  results.push({`);
    const projCols = this._getProjectionColumns(ast, [tableMeta]);
    for (const { name, alias: colAlias, idx, tableAlias } of projCols) {
      const outputName = colAlias || name;
      lines.push(`    ${JSON.stringify(outputName)}: values[${idx}],`);
    }
    lines.push(`  });`);

    if (limit) {
      lines.push(`  _count++;`);
    }

    lines.push(`}`);
    return lines;
  }

  /**
   * Generate code for hash join(s).
   */
  _genJoinQuery(ast, tables, plan) {
    const lines = [];
    const mainMeta = tables.main;
    const limit = ast.limit?.value;

    // Build hash tables for each join
    for (let i = 0; i < tables.joins.length; i++) {
      const jt = tables.joins[i];
      const joinCols = this._extractJoinColumns(jt.on);
      if (!joinCols) continue;

      const rightCol = this._resolveColumn(joinCols[1], jt.colMap) ??
                        this._resolveColumn(joinCols[0], jt.colMap);
      
      if (rightCol === null || rightCol === undefined) continue;

      lines.push(`// Build hash table for ${jt.alias}`);
      lines.push(`const ht_${i} = new Map();`);
      lines.push(`for (const {values} of ${jt.alias}_table.heap.scan()) {`);
      lines.push(`  const key = values[${rightCol}];`);
      lines.push(`  if (!ht_${i}.has(key)) ht_${i}.set(key, []);`);
      lines.push(`  ht_${i}.get(key).push(values);`);
      lines.push(`}`);
      lines.push('');
    }

    // Probe phase: scan main table, probe hash tables
    if (limit) {
      lines.push(`let _count = 0;`);
    }
    lines.push(`for (const {values: lv} of ${mainMeta.alias}_table.heap.scan()) {`);
    if (limit) {
      lines.push(`  if (_count >= ${limit}) break;`);
    }

    // Apply pre-join filter on main table
    if (ast.where) {
      const preFilter = this._genFilterExpr(ast.where, mainMeta.colMap, mainMeta.alias, true, 'lv');
      if (preFilter) {
        lines.push(`  if (!(${preFilter})) continue;`);
      }
    }

    // Nested probes into each hash table
    let currentIndent = 1;
    const varNames = ['lv']; // Track variable names for each join level

    for (let i = 0; i < tables.joins.length; i++) {
      const jt = tables.joins[i];
      const joinCols = this._extractJoinColumns(jt.on);
      if (!joinCols) continue;

      // Determine which column is from the left side
      const leftCol = this._resolveColumn(joinCols[0], mainMeta.colMap) ??
                       this._resolveColumn(joinCols[1], mainMeta.colMap);
      
      if (leftCol === null || leftCol === undefined) {
        // The left column might be from a previous join table
        // For multi-join, check against previous join tables
        let found = false;
        for (let j = 0; j < i; j++) {
          const prevJt = tables.joins[j];
          const prevIdx = this._resolveColumn(joinCols[0], prevJt.colMap) ??
                          this._resolveColumn(joinCols[1], prevJt.colMap);
          if (prevIdx !== null && prevIdx !== undefined) {
            lines.push(`${indent(currentIndent)}const matches_${i} = ht_${i}.get(rv_${j}[${prevIdx}]) || [];`);
            found = true;
            break;
          }
        }
        if (!found) continue;
      } else {
        const probeVar = i === 0 ? 'lv' : `rv_${i-1}`;
        const probeIdx = i === 0 ? leftCol : leftCol; // For first join, use main table
        lines.push(`${indent(currentIndent)}const matches_${i} = ht_${i}.get(${probeVar}[${probeIdx}]) || [];`);
      }

      if (jt.joinType === 'LEFT' || jt.joinType === 'LEFT OUTER') {
        lines.push(`${indent(currentIndent)}if (matches_${i}.length === 0) {`);
        // Emit null row for left join
        lines.push(`${indent(currentIndent + 1)}// Left join: no match`);
        const projCols = this._getProjectionColumns(ast, [mainMeta, ...tables.joins.slice(0, i + 1)]);
        lines.push(`${indent(currentIndent + 1)}results.push({`);
        for (const pc of projCols) {
          if (pc.tableIdx > i) {
            lines.push(`${indent(currentIndent + 2)}${JSON.stringify(pc.alias || pc.name)}: null,`);
          }
        }
        lines.push(`${indent(currentIndent + 1)}});`);
        if (limit) lines.push(`${indent(currentIndent + 1)}_count++;`);
        lines.push(`${indent(currentIndent)}} else {`);
        currentIndent++;
      }

      lines.push(`${indent(currentIndent)}for (const rv_${i} of matches_${i}) {`);
      currentIndent++;
      if (limit) {
        lines.push(`${indent(currentIndent)}if (_count >= ${limit}) break;`);
      }
      varNames.push(`rv_${i}`);
    }

    // Emit result row
    lines.push(`${indent(currentIndent)}results.push({`);
    const allTables = [mainMeta, ...tables.joins];
    const projCols = this._getProjectionColumns(ast, allTables);
    for (const pc of projCols) {
      const varName = pc.tableIdx === 0 ? 'lv' : `rv_${pc.tableIdx - 1}`;
      lines.push(`${indent(currentIndent + 1)}${JSON.stringify(pc.alias || pc.name)}: ${varName}[${pc.idx}],`);
    }
    lines.push(`${indent(currentIndent)}});`);
    if (limit) {
      lines.push(`${indent(currentIndent)}_count++;`);
    }

    // Close nested loops
    for (let i = tables.joins.length - 1; i >= 0; i--) {
      currentIndent--;
      lines.push(`${indent(currentIndent)}}`); // close for loop
      
      const jt = tables.joins[i];
      if (jt.joinType === 'LEFT' || jt.joinType === 'LEFT OUTER') {
        currentIndent--;
        lines.push(`${indent(currentIndent)}}`); // close else block
      }
    }

    lines.push(`}`); // close main scan loop
    return lines;
  }

  /**
   * Generate a filter expression as JavaScript code.
   * Returns a string that evaluates to boolean, or null if not compilable.
   */
  _genFilterExpr(expr, colMap, tableAlias, preJoinOnly = false, varName = 'values') {
    if (!expr) return null;

    switch (expr.type) {
      case 'COMPARE': {
        const leftCode = this._genValueExpr(expr.left, colMap, tableAlias, preJoinOnly, varName);
        const rightCode = this._genValueExpr(expr.right, colMap, tableAlias, preJoinOnly, varName);
        if (!leftCode || !rightCode) return null;

        const ops = { 'EQ': '===', 'NE': '!==', 'LT': '<', 'GT': '>', 'LE': '<=', 'GE': '>=' };
        const op = ops[expr.op];
        if (!op) return null;
        return `(${leftCode} ${op} ${rightCode})`;
      }
      case 'AND': {
        const left = this._genFilterExpr(expr.left, colMap, tableAlias, preJoinOnly, varName);
        const right = this._genFilterExpr(expr.right, colMap, tableAlias, preJoinOnly, varName);
        if (left && right) return `(${left} && ${right})`;
        return left || right;
      }
      case 'OR': {
        const left = this._genFilterExpr(expr.left, colMap, tableAlias, preJoinOnly, varName);
        const right = this._genFilterExpr(expr.right, colMap, tableAlias, preJoinOnly, varName);
        if (left && right) return `(${left} || ${right})`;
        return null;
      }
      case 'NOT': {
        const inner = this._genFilterExpr(expr.expr, colMap, tableAlias, preJoinOnly, varName);
        return inner ? `!(${inner})` : null;
      }
      case 'IS_NULL': {
        const val = this._genValueExpr(expr.expr || expr.left, colMap, tableAlias, preJoinOnly, varName);
        if (!val) return null;
        return `(${val} == null)`;
      }
      case 'IS_NOT_NULL': {
        const val = this._genValueExpr(expr.expr || expr.left, colMap, tableAlias, preJoinOnly, varName);
        if (!val) return null;
        return `(${val} != null)`;
      }
      default:
        return null;
    }
  }

  _genValueExpr(expr, colMap, tableAlias, preJoinOnly = false, varName = 'values') {
    if (!expr) return null;
    
    if (expr.type === 'column_ref') {
      // Pre-join filter: only handle columns from the main table
      if (preJoinOnly && expr.table && expr.table !== tableAlias) return null;
      
      const colName = expr.name;
      const idx = colMap[colName];
      if (idx !== undefined) return `${varName}[${idx}]`;
      
      // Try without table prefix
      if (!preJoinOnly) return null;
      return null;
    }
    
    if (expr.type === 'literal' || expr.value !== undefined) {
      const val = expr.value;
      return typeof val === 'string' ? JSON.stringify(val) : String(val);
    }
    
    if (typeof expr === 'string') return JSON.stringify(expr);
    if (typeof expr === 'number') return String(expr);
    
    return null;
  }

  /**
   * Determine output columns for projection.
   */
  _getProjectionColumns(ast, tableMetas) {
    const cols = [];
    
    if (!ast.columns || ast.columns.some(c => c === '*' || c.name === '*')) {
      // SELECT * — all columns from all tables
      // Track seen names to prefix with table alias on collision
      const seen = new Set();
      for (let ti = 0; ti < tableMetas.length; ti++) {
        const tm = tableMetas[ti];
        const colList = tm.columns || [];
        for (let ci = 0; ci < colList.length; ci++) {
          let outputName = colList[ci];
          if (seen.has(outputName)) {
            // Prefix with table alias to avoid collision
            outputName = `${tm.alias || tm.name}.${colList[ci]}`;
          }
          seen.add(colList[ci]);
          cols.push({
            name: colList[ci],
            alias: outputName !== colList[ci] ? outputName : null,
            idx: ci,
            tableIdx: ti,
            tableAlias: tm.alias || tm.name,
          });
        }
      }
      return cols;
    }

    // Specific columns
    for (const col of ast.columns) {
      const name = col.name;
      const tbl = col.table;
      const alias = col.alias;

      // Find the column in the table metadata
      for (let ti = 0; ti < tableMetas.length; ti++) {
        const tm = tableMetas[ti];
        if (tbl && tbl !== (tm.alias || tm.name)) continue;
        
        const colMap = tm.colMap || {};
        const idx = colMap[name];
        if (idx !== undefined) {
          cols.push({ name, alias, idx, tableIdx: ti, tableAlias: tm.alias || tm.name });
          break;
        }
      }
    }

    return cols;
  }

  _extractJoinColumns(onExpr) {
    if (!onExpr) return null;
    if (onExpr.type === 'COMPARE' && onExpr.op === 'EQ') {
      const left = onExpr.left?.name;
      const right = onExpr.right?.name;
      if (left && right) return [left, right];
    }
    if (onExpr.type === 'AND') {
      return this._extractJoinColumns(onExpr.left) || this._extractJoinColumns(onExpr.right);
    }
    return null;
  }

  _resolveColumn(name, colMap) {
    if (name in colMap) return colMap[name];
    // Try without table prefix
    const parts = name.split('.');
    if (parts.length > 1 && parts[1] in colMap) return colMap[parts[1]];
    return null;
  }

  /**
   * Get the generated source code for a query (for debugging/EXPLAIN).
   */
  explain(ast) {
    const compiled = this.compile(ast);
    if (!compiled) return 'Cannot compile this query';
    return compiled.source;
  }
}

/**
 * Helper: indent by n levels
 */
function indent(n) {
  return '  '.repeat(n);
}
