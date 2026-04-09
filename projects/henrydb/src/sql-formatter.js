// sql-formatter.js — Pretty-print SQL AST back to formatted SQL
// Supports: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX, etc.

const INDENT = '  ';

function formatExpr(expr) {
  if (!expr) return '';
  
  switch (expr.type) {
    case 'literal':
      if (typeof expr.value === 'string') return `'${expr.value.replace(/'/g, "''")}'`;
      if (expr.value === null) return 'NULL';
      return String(expr.value);
    
    case 'column_ref':
    case 'column':
      return expr.name;
    
    case 'COMPARE': {
      const ops = { EQ: '=', NEQ: '!=', LT: '<', GT: '>', LTE: '<=', GTE: '>=' };
      return `${formatExpr(expr.left)} ${ops[expr.op] || expr.op} ${formatExpr(expr.right)}`;
    }
    
    case 'AND':
      return `${formatExpr(expr.left)}\n${INDENT}AND ${formatExpr(expr.right)}`;
    
    case 'OR':
      return `(${formatExpr(expr.left)}\n${INDENT} OR ${formatExpr(expr.right)})`;
    
    case 'NOT':
      return `NOT ${formatExpr(expr.operand || expr.expr)}`;
    
    case 'BETWEEN':
      return `${formatExpr(expr.expr)} BETWEEN ${formatExpr(expr.low)} AND ${formatExpr(expr.high)}`;
    
    case 'IN_SUBQUERY': {
      return `${formatExpr(expr.left)} IN (\n${INDENT}${formatSelect(expr.right || expr.subquery, 1)}\n)`;
    }

    case 'IN':
      if (expr.subquery) return `${formatExpr(expr.expr)} IN (\n${INDENT}${formatSelect(expr.subquery, 1)}\n)`;
      return `${formatExpr(expr.expr)} IN (${(expr.values || []).map(formatExpr).join(', ')})`;
    
    case 'EXISTS':
      return `EXISTS (\n${INDENT}${formatSelect(expr.subquery, 1)}\n)`;
    
    case 'IS_NULL':
      return `${formatExpr(expr.expr)} IS NULL`;
    
    case 'IS_NOT_NULL':
      return `${formatExpr(expr.expr)} IS NOT NULL`;
    
    case 'LIKE':
      return `${formatExpr(expr.left)} LIKE ${formatExpr(expr.right)}`;
    
    case 'CASE':
    case 'case_expr': {
      let s = 'CASE';
      if (expr.operand) s += ` ${formatExpr(expr.operand)}`;
      for (const w of (expr.whens || [])) {
        s += `\n${INDENT}WHEN ${formatExpr(w.when || w.condition)} THEN ${formatExpr(w.then || w.result)}`;
      }
      const elseE = expr.else_expr || expr.elseResult;
      if (elseE) s += `\n${INDENT}ELSE ${formatExpr(elseE)}`;
      s += '\n  END';
      return s;
    }
    
    case 'function_call':
    case 'FUNCTION': {
      const name = expr.name || expr.function;
      const args = (expr.args || []).map(formatExpr).join(', ');
      const distinct = expr.distinct ? 'DISTINCT ' : '';
      let s = `${name.toUpperCase()}(${distinct}${args})`;
      if (expr.over) s += ` OVER (${formatWindow(expr.over)})`;
      return s;
    }
    
    case 'window_function': {
      const name = expr.name || expr.function;
      const args = (expr.args || []).map(formatExpr).join(', ');
      let s = `${name.toUpperCase()}(${args})`;
      if (expr.over) s += ` OVER (${formatWindow(expr.over)})`;
      return s;
    }
    
    case 'BINARY': {
      const ops = { ADD: '+', SUB: '-', MUL: '*', DIV: '/' };
      return `${formatExpr(expr.left)} ${ops[expr.op] || expr.op} ${formatExpr(expr.right)}`;
    }
    
    case 'unary_minus':
      return `-${formatExpr(expr.expr)}`;
    
    case 'CAST':
      return `CAST(${formatExpr(expr.expr)} AS ${expr.targetType || expr.dataType})`;
    
    case 'COALESCE':
      return `COALESCE(${(expr.args || []).map(formatExpr).join(', ')})`;
    
    case 'expression':
      return formatExpr(expr.expr);

    case 'subquery':
      return `(\n${INDENT}${formatSelect(expr.query || expr, 1)}\n)`;
    
    case 'star':
      return '*';
    
    default:
      // Fallback: try common patterns
      if (expr.op && expr.left && expr.right) {
        return `${formatExpr(expr.left)} ${expr.op} ${formatExpr(expr.right)}`;
      }
      if (expr.name) return expr.name;
      if (expr.value !== undefined) return String(expr.value);
      return '(?)';
  }
}

function formatWindow(over) {
  const parts = [];
  if (over.partitionBy) {
    parts.push(`PARTITION BY ${over.partitionBy.map(formatExpr).join(', ')}`);
  }
  if (over.orderBy) {
    parts.push(`ORDER BY ${over.orderBy.map(o => {
      const col = typeof o === 'string' ? o : (o.column || formatExpr(o.expr || o));
      const dir = (o.direction && o.direction !== 'ASC') ? ` ${o.direction}` : '';
      return `${col}${dir}`;
    }).join(', ')}`);
  }
  return parts.join(' ');
}

function formatColumn(col) {
  if (col.type === 'star' || col === '*') return '*';
  const expr = formatExpr(col.expr || col);
  const alias = col.alias ? ` AS ${col.alias}` : '';
  return `${expr}${alias}`;
}

function formatFrom(from, indent = 0) {
  if (!from) return '';
  const prefix = INDENT.repeat(indent);
  let s = from.table || from.name || '';
  if (from.subquery) {
    s = `(\n${prefix}${INDENT}${formatSelect(from.subquery, indent + 1)}\n${prefix})`;
  }
  if (from.alias) s += ` ${from.alias}`;
  return s;
}

function formatJoin(join, indent = 0) {
  const prefix = INDENT.repeat(indent);
  const type = (join.type || 'JOIN').toUpperCase();
  let s = `${type} ${formatFrom(join, indent)}`;
  if (join.on) s += `\n${prefix}${INDENT}ON ${formatExpr(join.on)}`;
  return s;
}

function formatSelect(ast, indent = 0) {
  const prefix = INDENT.repeat(indent);
  const lines = [];
  
  // WITH (CTEs)
  if (ast.cte || ast.with || ast.ctes) {
    const ctes = ast.cte || ast.with || ast.ctes;
    const cteParts = ctes.map(c => {
      const recursive = c.recursive ? 'RECURSIVE ' : '';
      return `${recursive}${c.name} AS (\n${prefix}${INDENT}${formatSelect(c.query || c.select, indent + 1)}\n${prefix})`;
    });
    lines.push(`WITH ${cteParts.join(',\n' + prefix + '     ')}`);
  }
  
  // SELECT
  const distinct = ast.distinct ? 'DISTINCT ' : '';
  const cols = (ast.columns || []).map(formatColumn).join(',\n' + prefix + '       ');
  lines.push(`SELECT ${distinct}${cols}`);
  
  // FROM
  if (ast.from) {
    lines.push(`FROM ${formatFrom(ast.from, indent)}`);
  }
  
  // JOINs
  if (ast.joins && ast.joins.length > 0) {
    for (const join of ast.joins) {
      lines.push(formatJoin(join, indent));
    }
  }
  
  // WHERE
  if (ast.where) {
    lines.push(`WHERE ${formatExpr(ast.where)}`);
  }
  
  // GROUP BY
  if (ast.groupBy) {
    const groups = ast.groupBy.map(g => typeof g === 'string' ? g : formatExpr(g)).join(', ');
    lines.push(`GROUP BY ${groups}`);
  }
  
  // HAVING
  if (ast.having) {
    lines.push(`HAVING ${formatExpr(ast.having)}`);
  }
  
  // ORDER BY
  if (ast.orderBy && ast.orderBy.length > 0) {
    const orders = ast.orderBy.map(o => {
      const col = o.column || formatExpr(o.expr || o);
      const dir = (o.direction && o.direction !== 'ASC') ? ` DESC` : '';
      return `${col}${dir}`;
    }).join(', ');
    lines.push(`ORDER BY ${orders}`);
  }
  
  // LIMIT / OFFSET
  if (ast.limit !== null && ast.limit !== undefined) {
    lines.push(`LIMIT ${ast.limit}`);
  }
  if (ast.offset !== null && ast.offset !== undefined) {
    lines.push(`OFFSET ${ast.offset}`);
  }
  
  return lines.join('\n' + prefix);
}

/**
 * Format a SQL AST back into readable SQL.
 * @param {Object} ast - Parsed AST from sql.parse()
 * @returns {string} Formatted SQL
 */
export function formatSQL(ast) {
  if (!ast) return '';
  
  switch (ast.type) {
    case 'SELECT': return formatSelect(ast) + ';';
    
    case 'INSERT': {
      let s = `INSERT INTO ${ast.table}`;
      if (ast.columns) s += ` (${ast.columns.join(', ')})`;
      if (ast.values || ast.rows) {
        const rows = (ast.values || ast.rows).map(row => `(${row.map(formatExpr).join(', ')})`);
        s += `\nVALUES ${rows.join(',\n       ')}`;
      }
      if (ast.select) s += `\n${formatSelect(ast.select)}`;
      return s + ';';
    }
    
    case 'UPDATE': {
      let s = `UPDATE ${ast.table}\nSET `;
      const sets = (ast.set || []).map(item => `${item.column} = ${formatExpr(item.value)}`);
      s += sets.join(',\n    ');
      if (ast.where) s += `\nWHERE ${formatExpr(ast.where)}`;
      return s + ';';
    }
    
    case 'DELETE': {
      let s = `DELETE FROM ${ast.table}`;
      if (ast.where) s += `\nWHERE ${formatExpr(ast.where)}`;
      return s + ';';
    }
    
    case 'CREATE_TABLE': {
      let s = `CREATE TABLE ${ast.name} (\n`;
      const cols = (ast.columns || []).map(col => {
        let def = `${INDENT}${col.name} ${(col.type || col.dataType || 'TEXT').toUpperCase()}`;
        if (col.primaryKey) def += ' PRIMARY KEY';
        if (col.notNull) def += ' NOT NULL';
        if (col.default !== undefined) def += ` DEFAULT ${formatExpr({ type: 'literal', value: col.default })}`;
        return def;
      });
      s += cols.join(',\n');
      s += '\n)';
      return s + ';';
    }
    
    case 'CREATE_INDEX': {
      const unique = ast.unique ? 'UNIQUE ' : '';
      return `CREATE ${unique}INDEX ${ast.name} ON ${ast.table}(${(ast.columns || []).join(', ')});`;
    }
    
    case 'CREATE_VIEW': {
      return `CREATE VIEW ${ast.name} AS\n${formatSelect(ast.query)};`;
    }
    
    default:
      return `-- Unsupported AST type: ${ast.type}`;
  }
}

/**
 * Parse and reformat SQL string.
 * @param {string} sql - Raw SQL string
 * @param {Function} parseFn - Parser function (parse from sql.js)
 * @returns {string} Formatted SQL
 */
export function format(sql, parseFn) {
  const ast = parseFn(sql);
  return formatSQL(ast);
}
