// select-inner.js — Core SELECT execution extracted from db.js
// Functions take "db" as first parameter (database context)

import { pushdownPredicates } from './pushdown.js';
import { canVectorize, vectorizedGroupBy } from './vectorized-bridge.js';
import { getCompiledExpr } from './compiled-expr.js';

// Built-in table-valued functions
function executeBuiltinTVF(funcName, args, db) {
  const argValues = args.map(a => {
    if (a.type === 'literal' || a.type === 'number') return a.value;
    if (a.type === 'string') return a.value;
    // Try to evaluate
    try { return db._evalValue(a, {}); } catch { return a.value; }
  });
  
  switch (funcName.toLowerCase()) {
    case 'json_each': {
      const jsonStr = argValues[0];
      const rootPath = argValues[1] || '$';
      if (jsonStr == null) return { rows: [] };
      try {
        let obj = typeof jsonStr === 'string' ? JSON.parse(jsonStr) : jsonStr;
        // Navigate to root path if not $
        if (rootPath !== '$') {
          const parts = rootPath.replace(/^\$\.?/, '').split(/\.|\[(\d+)\]/).filter(Boolean);
          for (const p of parts) {
            obj = obj?.[/^\d+$/.test(p) ? parseInt(p) : p];
          }
        }
        const rows = [];
        if (Array.isArray(obj)) {
          for (let i = 0; i < obj.length; i++) {
            const v = obj[i];
            const type = v === null ? 'null' : Array.isArray(v) ? 'array' : typeof v === 'object' ? 'object' : typeof v;
            const atom = (typeof v === 'object' && v !== null) ? null : v;
            rows.push({
              key: i,
              value: typeof v === 'object' && v !== null ? JSON.stringify(v) : v,
              type,
              atom,
              id: i,
              parent: null,
              fullkey: `${rootPath}[${i}]`,
              path: rootPath,
            });
          }
        } else if (typeof obj === 'object' && obj !== null) {
          let id = 0;
          for (const [key, v] of Object.entries(obj)) {
            const type = v === null ? 'null' : Array.isArray(v) ? 'array' : typeof v === 'object' ? 'object' : typeof v;
            const atom = (typeof v === 'object' && v !== null) ? null : v;
            rows.push({
              key,
              value: typeof v === 'object' && v !== null ? JSON.stringify(v) : v,
              type,
              atom,
              id: id++,
              parent: null,
              fullkey: `${rootPath}.${key}`,
              path: rootPath,
            });
          }
        }
        return { rows };
      } catch { return { rows: [] }; }
    }
    
    case 'generate_series': {
      const start = Number(argValues[0]) || 0;
      const stop = Number(argValues[1]) || 0;
      const step = Number(argValues[2]) || 1;
      const rows = [];
      for (let v = start; step > 0 ? v <= stop : v >= stop; v += step) {
        rows.push({ value: v });
      }
      return { rows };
    }
    
    case 'json_tree': {
      // Recursive version of json_each
      const jsonStr = argValues[0];
      const rootPath = argValues[1] || '$';
      if (jsonStr == null) return { rows: [] };
      try {
        const obj = typeof jsonStr === 'string' ? JSON.parse(jsonStr) : jsonStr;
        const rows = [];
        let nextId = 0;
        const walk = (value, path, parentId) => {
          const id = nextId++;
          const type = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value === 'object' ? 'object' : typeof value;
          const atom = (typeof value === 'object' && value !== null) ? null : value;
          const key = path === rootPath ? null : path.includes('[') ? parseInt(path.match(/\[(\d+)\]$/)?.[1] || '0') : path.split('.').pop();
          rows.push({
            key,
            value: typeof value === 'object' && value !== null ? JSON.stringify(value) : value,
            type,
            atom,
            id,
            parent: parentId,
            fullkey: path,
            path: path === rootPath ? '$' : path.replace(/\.?[^.\[]+(\[\d+\])?$/, '') || '$',
          });
          if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
              walk(value[i], `${path}[${i}]`, id);
            }
          } else if (typeof value === 'object' && value !== null) {
            for (const [k, v] of Object.entries(value)) {
              walk(v, `${path}.${k}`, id);
            }
          }
        };
        walk(obj, rootPath, null);
        return { rows };
      } catch { return { rows: [] }; }
    }
    
    default:
      return null; // Not a built-in TVF
  }
}
import { sqliteCompare } from './type-affinity.js';

// Deduplicate column name within a result row object
function dedup(name, result) {
  if (!(name in result)) return name;
  let suffix = 1;
  while (`${name}_${suffix}` in result) suffix++;
  return `${name}_${suffix}`;
}

/**
 * Apply WHERE filter using compiled expression when possible, falling back to _evalExpr.
 */
function _filterWhere(db, ast, rows) {
  if (!ast.where) return rows;
  const compiled = getCompiledExpr(ast.where);
  if (compiled) return rows.filter(compiled);
  return rows.filter(row => db._evalExpr(ast.where, row));
}

export function selectInner(db, ast) {
  // Resolve limitExpr (e.g., subqueries) at execution time
  if (ast.limitExpr != null && ast.limit == null) {
    const limitVal = db._evalValue(ast.limitExpr, {});
    if (typeof limitVal === 'number') {
      ast = { ...ast, limit: limitVal, limitExpr: null };
    }
  }
  // Handle SELECT without FROM (e.g., SELECT 1 AS n)
  if (!ast.from) {
    // Check WHERE clause (e.g., SELECT 1 WHERE 0 → empty result)
    if (ast.where) {
      const whereResult = db._evalExpr(ast.where, {});
      if (!whereResult) {
        // Check if any columns are aggregates — aggregates on empty still return a row
        const hasAgg = ast.columns.some(c => c.type === 'aggregate');
        if (hasAgg) {
          return { type: 'ROWS', rows: [db._computeAggregates(ast.columns, [])] };
        }
        return { type: 'ROWS', rows: [] };
      }
    }
    
    // Check if any columns are aggregates — if so, use _computeAggregates with 1 implicit row
    const hasAgg = ast.columns.some(c => c.type === 'aggregate');
    if (hasAgg) {
      const implicitRows = [{}]; // One implicit row for aggregate evaluation
      return { type: 'ROWS', rows: [db._computeAggregates(ast.columns, implicitRows)] };
    }
    const row = {};
    let noFromExprIdx = 0;
    for (const col of ast.columns) {
      if (col.type === 'expression') {
        const name = col.alias || `expr_${noFromExprIdx++}`;
        row[name] = db._evalValue(col.expr, {});
      } else if (col.type === 'scalar_subquery') {
        const name = col.alias || 'subquery';
        const subResult = db._evalSubquery(col.subquery, {});
        row[name] = subResult.length > 0 ? Object.values(subResult[0])[0] : null;
      } else if (col.type === 'column') {
        const name = col.alias || String(col.name);
        row[name] = typeof col.name === 'number' ? col.name : col.name;
      } else if (col.type === 'function') {
        const name = dedup(col.alias || `${col.func}(...)`, row);
        row[name] = db._evalFunction(col.func, col.args, {});
      }
    }
    return { type: 'ROWS', rows: [row] };
  }

  // Check if FROM is GENERATE_SERIES
  let tableName = ast.from.table;
  if (tableName === '__generate_series') {
    const start = db._evalValue(ast.from.start, {});
    const stop = db._evalValue(ast.from.stop, {});
    const step = ast.from.step ? db._evalValue(ast.from.step, {}) : 1;
    const colName = ast.from.columnAliases?.[0] || 'value';
    let rows = [];
    if (step > 0) {
      for (let i = start; i <= stop; i += step) {
        rows.push({ [colName]: i });
      }
    } else if (step < 0) {
      for (let i = start; i >= stop; i += step) {
        rows.push({ [colName]: i });
      }
    }
    // Apply WHERE
    rows = _filterWhere(db, ast, rows);
    
    // Handle aggregates / GROUP BY on GENERATE_SERIES results
    const gsHasAggregates = ast.columns.some(c => c.type === 'aggregate');
    const gsHasWindow = db._columnsHaveWindow(ast.columns);
    if (ast.groupBy) {
      return db._selectWithGroupBy(ast, rows);
    }
    if (gsHasAggregates) {
      return { type: 'ROWS', rows: [db._computeAggregates(ast.columns, rows)] };
    }
    if (gsHasWindow) {
      rows = db._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
    }
    
    // Apply columns
    return db._applySelectColumns(ast, rows);
  }

  // JSON_EACH / JSON_TREE: expand JSON to rows
  if (tableName === '__json_each' || tableName === '__json_tree') {
    const jsonVal = db._evalValue(ast.from.jsonExpr, {});
    const pathVal = ast.from.pathExpr ? db._evalValue(ast.from.pathExpr, {}) : '$';
    // Call the TVF directly with evaluated values
    const tvfResult = executeBuiltinTVF(ast.from.tvfName, [
      { type: 'literal', value: jsonVal },
      { type: 'literal', value: pathVal }
    ], db);
    let rows = tvfResult ? tvfResult.rows : [];
    
    // Apply WHERE
    rows = _filterWhere(db, ast, rows);
    
    const hasAgg = ast.columns.some(c => c.type === 'aggregate');
    if (ast.groupBy) return db._selectWithGroupBy(ast, rows);
    if (hasAgg) return { type: 'ROWS', rows: [db._computeAggregates(ast.columns, rows)] };
    
    return db._applySelectColumns(ast, rows);
  }

  // UNNEST: expand array to rows
  if (tableName === '__unnest') {
    const arrayVal = db._evalValue(ast.from.arrayExpr, {});
    let arr = Array.isArray(arrayVal) ? arrayVal : [];
    if (typeof arrayVal === 'string') {
      try { arr = JSON.parse(arrayVal); } catch { arr = []; }
    }
    const colName = ast.from.columnAlias || 'value';
    let rows = arr.map(v => ({ [colName]: v }));
    
    rows = _filterWhere(db, ast, rows);
    if (ast.groupBy) return db._selectWithGroupBy(ast, rows);
    
    const hasAgg = ast.columns.some(c => c.type === 'aggregate');
    if (hasAgg) return { type: 'ROWS', rows: [db._computeAggregates(ast.columns, rows)] };
    
    if (ast.orderBy) rows = db._sortRows(rows, ast.orderBy);
    if (ast.limit != null && ast.limit >= 0) rows = rows.slice(ast.offset || 0, (ast.offset || 0) + ast.limit);
    return { type: 'ROWS', rows };
  }

  // Check if FROM is a function call (table-returning function)
  if (tableName === '__func_call') {
    const funcName = ast.from.func;
    
    // Check for built-in TVFs first
    const builtinTVF = executeBuiltinTVF(funcName, ast.from.args, db);
    if (builtinTVF) {
      const funcAlias = ast.from.alias || funcName;
      ast.from.subquery = { _virtualRows: builtinTVF.rows };
      ast.from.alias = funcAlias;
      tableName = '__subquery';
    } else {
      const funcDef = db._functions?.get(funcName);
      if (!funcDef) throw new Error(`Function ${funcName} does not exist`);
      if (funcDef.returnType !== 'TABLE') throw new Error(`Function ${funcName} does not return TABLE`);
    
      // Evaluate arguments
      const evalArgs = ast.from.args.map(a => db._evalValue(a, {}));
      const result = db._callUserFunction(funcDef, evalArgs);
      let funcRows = result.rows || [];
    
      // Transform to a subquery-style result and process as subquery
      const funcAlias = ast.from.alias || funcName;
      ast.from.subquery = { _virtualRows: funcRows };
      ast.from.alias = funcAlias;
      // Fall through to the __subquery handler
      tableName = '__subquery';
    }
  }

  // VALUES clause in FROM: (VALUES (1, 'a'), (2, 'b')) AS t(id, name)
  if (tableName === '__values') {
    const tuples = ast.from.tuples;
    const columnAliases = ast.from.columnAliases;
    const alias = ast.from.alias || '__values';
    
    let rows = tuples.map(tuple => {
      const row = {};
      tuple.forEach((val, i) => {
        const colName = columnAliases?.[i] || `column${i + 1}`;
        if (val.type === 'number') row[colName] = val.value;
        else if (val.type === 'string') row[colName] = val.value;
        else if (val.type === 'literal' && val.value === null) row[colName] = null;
        else if (val.type === 'null' || val.type === 'NULL') row[colName] = null;
        else if (val.type === 'literal') row[colName] = val.value;
        else if (val.type === 'column_ref') {
          // Unquoted identifier parsed as column_ref — treat as string value
          row[colName] = val.name;
        } else if (val.type === 'COMPARE' || val.type === 'MATH' || val.type === 'function_call') {
          row[colName] = db._evalExpression(val, {});
        } else {
          row[colName] = val.value ?? val;
        }
      });
      return row;
    });
    
    // Add alias-prefixed columns
    if (alias) {
      rows = rows.map(row => {
        const newRow = { ...row };
        for (const key of Object.keys(row)) {
          if (!key.includes('.')) newRow[`${alias}.${key}`] = row[key];
        }
        return newRow;
      });
    }
    
    // Create a temporary AST that treats this like a subquery result
    ast.from = { table: '__subquery', alias, subquery: { _virtualRows: rows } };
    tableName = '__subquery';
  }

  // Check if FROM is a subquery
  if (tableName === '__subquery') {
    const subAst = ast.from.subquery;
    // Handle UNION/INTERSECT/EXCEPT in derived tables
          // Handle virtual rows from table-returning functions
    let rows;
    if (subAst._virtualRows) {
      rows = subAst._virtualRows;
    } else {
      const subResult = (subAst.type === 'UNION' || subAst.type === 'INTERSECT' || subAst.type === 'EXCEPT')
        ? db.execute_ast(subAst)
        : db._select(subAst);
      rows = subResult.rows || [];
    }
    
    // Add qualified column names (sub.col) so alias-prefixed references work
    const subAlias = ast.from.alias;
    if (subAlias) {
      rows = rows.map(row => {
        const newRow = { ...row };
        for (const key of Object.keys(row)) {
          if (!key.includes('.')) {
            newRow[`${subAlias}.${key}`] = row[key];
          }
        }
        return newRow;
      });
      // Strip table alias prefix from column references (e.g., sub.col → col)
      const prefix = subAlias + '.';
      for (const col of ast.columns) {
        if (col.name && col.name.startsWith(prefix)) {
          col.name = col.name.substring(prefix.length);
        }
      }
      if (ast.orderBy) {
        for (const o of ast.orderBy) {
          if (typeof o.column === 'string' && o.column.startsWith(prefix)) {
            o.column = o.column.substring(prefix.length);
          }
        }
      }
    }
    rows = _filterWhere(db, ast, rows);
    for (const join of ast.joins || []) {
      rows = db._executeJoin(rows, join, ast.from.alias || '__subquery');
    }
    // Handle aggregates / GROUP BY on subquery results
    const sqHasAggregates = ast.columns.some(c => c.type === 'aggregate');
    const sqHasWindow = db._columnsHaveWindow(ast.columns);
    if (ast.groupBy) {
      return db._selectWithGroupBy(ast, rows);
    }
    if (sqHasAggregates) {
      return { type: 'ROWS', rows: [db._computeAggregates(ast.columns, rows)] };
    }
    if (sqHasWindow) {
      rows = db._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
    }
    return db._applySelectColumns(ast, rows);
  }

  // Check if FROM references a view
  if (db.views.has(tableName)) {
    const viewDef = db.views.get(tableName);
    // Execute view query or use materialized rows (for recursive CTEs)
    let rows;
    if (viewDef.isMaterialized && db.tables.has(tableName)) {
      // Materialized view: read from stored table
      const mvTable = db.tables.get(tableName);
      rows = [];
      for (const { values } of mvTable.heap.scan()) {
        const row = db._valuesToRow(values, mvTable.schema, tableName);
        rows.push(row);
      }
    } else if (viewDef.materializedRows) {
      rows = [...viewDef.materializedRows];
    } else {
      // Execute the view query — handle UNION/INTERSECT/EXCEPT via execute_ast
      const viewResult = viewDef.query.type === 'UNION' || viewDef.query.type === 'INTERSECT' || viewDef.query.type === 'EXCEPT'
        ? db.execute_ast(viewDef.query)
        : db._select(viewDef.query);
      rows = viewResult.rows;
      // Apply CTE column renaming if specified
      if (viewDef.cteColumns) {
        db._renameCTEColumns(rows, viewDef.cteColumns);
      }
    }

    // Add qualified column names (alias.col) for alias-prefixed references
    const viewAlias = ast.from.alias || ast.from.table;
    if (viewAlias) {
      rows = rows.map(row => {
        const newRow = { ...row };
        for (const key of Object.keys(row)) {
          if (!key.includes('.')) {
            newRow[`${viewAlias}.${key}`] = row[key];
          }
        }
        return newRow;
      });
    }

    // Apply WHERE — only if no JOINs, otherwise apply after JOINs
    if (ast.where && (!ast.joins || ast.joins.length === 0)) {
      rows = _filterWhere(db, ast, rows);
    }

    // Handle JOINs on view results
    for (const join of ast.joins || []) {
      rows = db._executeJoin(rows, join, viewAlias || tableName);
    }

    // Apply WHERE after JOINs (when JOINs exist)
    if (ast.where && ast.joins && ast.joins.length > 0) {
      rows = _filterWhere(db, ast, rows);
    }

    // Handle aggregates / GROUP BY on view results
    // Validate no nested aggregates (e.g., SUM(COUNT(*)))
    db._validateNoNestedAggregates(ast.columns);
    
    const hasAggregates = ast.columns.some(c => 
      c.type === 'aggregate' || 
      db._exprContainsAggregate(c.expr) ||
      (c.type === 'function' && c.args && c.args.some(a => db._exprContainsAggregate(a)))
    );
    const hasWindow = db._columnsHaveWindow(ast.columns);
    if (ast.groupBy) {
      return db._selectWithGroupBy(ast, rows);
    }
    if (hasAggregates) {
      return { type: 'ROWS', rows: [db._computeAggregates(ast.columns, rows)] };
    }
    if (hasWindow) {
      rows = db._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
    }

    // ORDER BY — pre-compute aliased expressions for sort access
    if (ast.orderBy) {
      db._preComputeOrderByAliases(ast, rows);
      rows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          const av = db._orderByValue(column, a);
          const bv = db._orderByValue(column, b);
          const aNull = av === null || av === undefined;
          const bNull = bv === null || bv === undefined;
          if (aNull && bNull) continue;
          if (aNull) return direction === 'DESC' ? 1 : -1;
          if (bNull) return direction === 'DESC' ? -1 : 1;
          const cmp = sqliteCompare(av, bv);
          if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
        }
        return 0;
      });
    }

    if (ast.offset) rows = rows.slice(Math.max(0, ast.offset));
    if (ast.limit != null && ast.limit >= 0) rows = rows.slice(0, ast.limit);

    // Project columns
    if (ast.columns.length === 1 && ast.columns[0]?.type === 'star') {
      // SELECT * — handle column name collisions from joins
      rows = rows.map(row => db._projectStarRow(row));
    } else {
      rows = rows.map(row => {
        const result = {};
        let viewExprIdx = 0;
        for (const col of ast.columns) {
          if (col.type === 'star') {
            for (const [key, val] of Object.entries(row)) {
              if (!key.includes('.') && !key.startsWith('__')) result[key] = val;
            }
          } else if (col.type === 'function') {
            const name = col.alias || `${col.func}(...)`;
            result[name] = db._evalFunction(col.func, col.args, row);
          } else if (col.type === 'expression') {
            const name = col.alias || `expr_${viewExprIdx++}`;
            result[name] = db._evalValue(col.expr, row);
          } else if (col.type === 'window') {
            const name = col.alias || `${col.func}(${col.arg || ''})`;
            result[name] = row[`__window_${name}`];
          } else {
            const rawName = col.alias || col.name;
            // Strip table alias prefix for output key: ds.dept_name → dept_name
            const name = rawName.includes('.') ? rawName.split('.').pop() : rawName;
            result[name] = row[col.name] !== undefined ? row[col.name] : row[rawName] !== undefined ? row[rawName] : row[name];
          }
        }
        return result;
      });
    }

    // DISTINCT
    if (ast.distinct) {
      const seen = new Set();
      rows = rows.filter(row => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    return { type: 'ROWS', rows };
  }

  const table = db.tables.get(ast.from.table);
  if (!table) throw new Error(`Table ${ast.from.table} not found`);

  // Validate column references against table schema (single-table, non-star queries)
  const hasJoins = ast.joins && ast.joins.length > 0;
  if (!hasJoins && table.schema) {
    const schemaColNames = new Set(table.schema.map(c => c.name.toLowerCase()));
    const tableAlias = ast.from.alias || ast.from.table;
    // Validate SELECT columns
    for (const col of ast.columns) {
      if (col.type === 'star' || col.type === 'qualified_star') continue;
      if (col.type === 'aggregate' || col.type === 'function' || col.type === 'expression' || col.type === 'window' || col.type === 'scalar_subquery') continue;
      if (col.type === 'column' && typeof col.name === 'string') {
        const rawName = col.name.includes('.') ? col.name.split('.').pop() : col.name;
        if (!schemaColNames.has(rawName.toLowerCase())) {
          throw new Error(`Column "${rawName}" does not exist in table "${ast.from.table}"`);
        }
      }
    }
    // Validate WHERE column references
    if (ast.where) {
      db._validateColumnRefs(ast.where, schemaColNames, ast.from.table, tableAlias);
    }
  }

  let rows = [];

  // Apply predicate pushdown for joins: push WHERE filters to individual table scans
  let workingAst = ast;
  if (hasJoins && ast.where) {
    const { ast: pushedAst, pushed } = pushdownPredicates(ast);
    if (pushed > 0) {
      workingAst = pushedAst;
    }
  }

  // Try index scan for simple equality WHERE clauses (only when no JOINs)
  if (!hasJoins) {
    // Set requested columns for index-only scan detection
    db._requestedColumns = ast.columns[0]?.type === 'star' ? null : 
      ast.columns.filter(c => c.type === 'column' && typeof c.name === 'string').map(c => {
        const name = c.name;
        return name.includes('.') ? name.split('.').pop() : name;
      });
    const indexScan = db._tryIndexScan(table, ast.where, ast.from.alias || ast.from.table);
    db._requestedColumns = null;
    
    // Cost-based decision: should we use the index or do a seq scan?
    let useIndexScan = !!indexScan;
    if (indexScan && !indexScan.btreeLookup) {
      const totalRows = db._estimateRowCount(table);
      const estimatedResultRows = indexScan.rows.length;
      const costComparison = db._compareScanCosts(totalRows, estimatedResultRows);
      useIndexScan = costComparison.useIndex;
    }
    
    if (useIndexScan) {
      rows = indexScan.rows;
      // Apply remaining where conditions (if any beyond what the index handled)
      if (indexScan.residual) {
        rows = rows.filter(row => db._evalExpr(indexScan.residual, row));
      }
    } else {
      // Full table scan with optional early LIMIT
      // Can push limit into scan when: no ORDER BY, no GROUP BY, no DISTINCT, no HAVING, no windows
      const canEarlyLimit = ast.limit != null && !ast.orderBy && !ast.groupBy && !ast.distinct &&
        !ast.having && !db._columnsHaveWindow(ast.columns);
      const earlyLimit = canEarlyLimit && ast.limit >= 0 ? (ast.limit + (ast.offset || 0)) : Infinity;
      
      const compiledWhere = ast.where ? (getCompiledExpr(ast.where) || ((row) => db._evalExpr(ast.where, row))) : null;
      for (const { pageId, slotIdx, values } of table.heap.scan()) {
        const row = db._valuesToRow(values, table.schema, ast.from.alias || ast.from.table);
        if (compiledWhere && !compiledWhere(row)) continue;
        rows.push(row);
        if (rows.length >= earlyLimit) break;
      }
    }
  } else {
    // With JOINs: scan FROM table, apply pushed filter if available
    for (const { pageId, slotIdx, values } of table.heap.scan()) {
      const row = db._valuesToRow(values, table.schema, workingAst.from.alias || workingAst.from.table);
      rows.push(row);
    }
    // Apply pushed-down filter for the FROM table
    if (workingAst.from.filter) {
      rows = rows.filter(row => db._evalExpr(workingAst.from.filter, row));
    }
  }

  // Apply TABLESAMPLE (random row sampling)
  if (ast.from.tablesample) {
    const pct = ast.from.tablesample.percentage / 100;
    rows = rows.filter(() => Math.random() < pct);
  }

  // Handle JOINs — optimize join order if possible
  let joinList = workingAst.joins || [];
  if (joinList.length >= 2) {
    const fromTableName = workingAst.from.table;
    if (fromTableName) {
      joinList = db._optimizeJoinOrder(fromTableName, joinList);
    }
  }
  for (const join of joinList) {
    rows = db._executeJoin(rows, join, workingAst.from.alias || workingAst.from.table);
  }

  // WHERE filter after JOINs (only remaining predicates)
  if (hasJoins && workingAst.where) {
    rows = rows.filter(row => db._evalExpr(workingAst.where, row));
  }

  // Validate no nested aggregates
  db._validateNoNestedAggregates(ast.columns);
  
  // Validate no window functions in WHERE/HAVING
  db._validateNoWindowInWhere(ast.where, 'WHERE');
  
  // Aggregates / GROUP BY / Window functions
  const hasAggregates = ast.columns.some(c =>
    c.type === 'aggregate' || 
    db._exprContainsAggregate(c.expr) ||
    (c.type === 'function' && c.args && c.args.some(a => db._exprContainsAggregate(a)))
  );
  const hasWindow = db._columnsHaveWindow(ast.columns);

  if (ast.groupBy) {
    return _tryVectorizedGroupBy(db, ast, rows) || db._selectWithGroupBy(ast, rows);
  }
  if (hasAggregates && !hasWindow) {
    const aggRow = db._computeAggregates(ast.columns, rows);
    // HAVING without GROUP BY: entire result is one group
    if (ast.having) {
      // Evaluate HAVING using the group computation path
      const computeAgg = (func, arg, distinct) => {
        return db._computeSingleAggregate(func, arg, rows, distinct);
      };
      const passes = db._evalGroupCond(ast.having, rows, aggRow, computeAgg);
      if (!passes) {
        return { type: 'ROWS', rows: [] };
      }
    }
    return { type: 'ROWS', rows: [aggRow] };
  }

  // Window functions: compute window values before projection
  if (hasWindow) {
    rows = db._computeWindowFunctions(ast.columns, rows, ast.windowDefs);
  }

  // Build alias→expression map for ORDER BY resolution
  const aliasExprs = new Map();
  for (const col of ast.columns) {
    if (col.alias) {
      if (col.type === 'expression') {
        aliasExprs.set(col.alias, col.expr);
      } else if (col.type === 'function') {
        aliasExprs.set(col.alias, col);
      } else if (col.type === 'column') {
        // Simple column alias: val as v → resolve as column_ref
        aliasExprs.set(col.alias, { type: 'column_ref', name: col.name });
      } else if (col.type === 'aggregate') {
        aliasExprs.set(col.alias, col);
      } else if (col.type === 'scalar_subquery') {
        aliasExprs.set(col.alias, col);
      }
    }
  }

  // ORDER BY
  if (ast.orderBy) {
    // Validate ORDER BY columns exist (when we have rows to check against)
    if (rows.length > 0) {
      for (const { column } of ast.orderBy) {
        if (typeof column === 'number') continue; // Numeric ref — validated later
        if (typeof column === 'object') continue; // Expression — validated during eval
        if (aliasExprs.has(column)) continue; // SELECT alias
        const sample = rows[0];
        if (sample[`__window_${column}`] !== undefined) continue; // Window function
        if (column in sample) continue; // Direct column
        const resolved = db._resolveColumn(column, sample);
        if (resolved === undefined && !Object.keys(sample).some(k => k.toLowerCase() === column.toLowerCase())) {
          throw new Error(`no such column: ${column}`);
        }
      }
    }
    rows.sort((a, b) => {
      for (const { column, direction, nulls } of ast.orderBy) {
        let av, bv;
        if (typeof column === 'number') {
          // Numeric column reference (ORDER BY 1, 2, etc.)
          // Resolve using SELECT column list
          const selCol = ast.columns[column - 1];
          if (selCol) {
            const colName = selCol.alias || selCol.name;
            av = a[colName] !== undefined ? a[colName] : db._resolveColumn(colName, a);
            bv = b[colName] !== undefined ? b[colName] : db._resolveColumn(colName, b);
          }
        } else if (typeof column === 'object' && column !== null) {
          // Expression node (ORDER BY -val, ORDER BY col + 1, etc.)
          av = db._evalValue(column, a);
          bv = db._evalValue(column, b);
        } else if (aliasExprs.has(column)) {
          // Prefer SELECT alias expressions over raw column values
          // This handles cases like COALESCE(a.x, b.x) as x where 'x' exists as raw column
          const expr = aliasExprs.get(column);
          if (expr.type === 'function') {
            av = db._evalFunction(expr.func, expr.args, a);
            bv = db._evalFunction(expr.func, expr.args, b);
          } else {
            av = db._evalValue(expr, a);
            bv = db._evalValue(expr, b);
          }
        } else if (a[`__window_${column}`] !== undefined) {
          // Window function alias — resolve from computed window columns
          av = a[`__window_${column}`];
          bv = b[`__window_${column}`];
        } else if (column in a) {
          // Direct key match (simple column reference)
          av = a[column];
          bv = b[column];
        } else {
          av = db._resolveColumn(column, a);
          bv = db._resolveColumn(column, b);
        }
        // NULLS FIRST: nulls sort before non-nulls; NULLS LAST: nulls sort after
        // Default: NULLS FIRST for ASC, NULLS LAST for DESC (PostgreSQL convention)
        const nullsFirst = nulls === 'FIRST' || (nulls == null && direction !== 'DESC');
        if (av == null && bv == null) continue;
        if (av == null) return nullsFirst ? -1 : 1;
        if (bv == null) return nullsFirst ? 1 : -1;
        const cmp = sqliteCompare(av, bv);
        if (cmp !== 0) return direction === 'DESC' ? -cmp : cmp;
      }
      return 0;
    });
  }

  // OFFSET (before LIMIT, but LIMIT deferred until after DISTINCT)
  if (ast.offset && !ast.distinct) rows = rows.slice(Math.max(0, ast.offset));

  // LIMIT (only apply before projection if no DISTINCT)
  if (ast.limit != null && ast.limit >= 0 && !ast.distinct) rows = rows.slice(0, ast.limit);

  // Materialize correlated scalar subqueries: pre-compute once with GROUP BY
  // instead of re-executing per row. Transforms O(n*m) to O(n+m).
  const materializedSubqueries = new Map(); // columnIndex → Map(correlationKey → value)
  for (let ci = 0; ci < ast.columns.length; ci++) {
    const col = ast.columns[ci];
    if (col.type !== 'scalar_subquery') continue;
    const sub = col.subquery;
    if (!sub || !sub.where) continue;
    
    // Detect correlation: WHERE inner_col = outer_ref pattern
    const corr = db._detectCorrelation(sub.where, rows[0]);
    if (!corr) continue;
    
    // Build a materialized lookup: execute subquery once with GROUP BY on correlation column
    try {
      const tableName = sub.from?.table;
      if (!tableName) continue;
      
      // Get the original aggregate column text (e.g., COUNT(*), SUM(amount))
      const aggCol = sub.columns[0];
      let aggExpr;
      if (aggCol.type === 'aggregate') {
        aggExpr = `${aggCol.func}(${aggCol.distinct ? 'DISTINCT ' : ''}${aggCol.arg || '*'})`;
      } else {
        continue; // Not an aggregate — can't materialize
      }
      
      // Construct and execute materialized query
      const matSql = `SELECT ${corr.innerCol} AS __corr_key, ${aggExpr} AS __corr_val FROM ${tableName} GROUP BY ${corr.innerCol}`;
      const materializedResult = db.execute(matSql);
      const lookup = new Map();
      for (const r of (materializedResult.rows || materializedResult)) {
        lookup.set(r.__corr_key, r.__corr_val);
      }
      // For COUNT aggregates, default to 0 (not null) when no matching rows
      const defaultVal = aggCol.func?.toUpperCase() === 'COUNT' ? 0 : null;
      materializedSubqueries.set(ci, { lookup, outerCol: corr.outerCol, defaultValue: defaultVal });
    } catch (e) {
      // Materialization failed — fall back to per-row execution
      continue;
    }
  }

  // Project columns
  const projected = rows.map(row => {
    const result = {};
    let exprIdx = 0;
    let hadStar = false;
    let colIdx = -1;
    for (const col of ast.columns) {
      colIdx++;
      if (col.type === 'star') {
        // Include all columns, handling collisions
        hadStar = true;
        Object.assign(result, db._projectStarRow(row));
      } else if (col.type === 'qualified_star') {
        // Include all columns from specified table
        const prefix = col.table + '.';
        for (const [key, val] of Object.entries(row)) {
          if (key.startsWith(prefix)) {
            result[key.slice(prefix.length)] = val;
          }
        }
      } else if (col.type === 'function') {
        const name = dedup(col.alias || `${col.func}(...)`, result);
        result[name] = db._evalFunction(col.func, col.args, row);
      } else if (col.type === 'expression') {
        const name = col.alias || `expr_${exprIdx++}`;
        result[name] = db._evalValue(col.expr, row);
      } else if (col.type === 'scalar_subquery') {
        const name = col.alias || 'subquery';
        // Check for materialized subquery (pre-computed for correlation optimization)
        const mat = materializedSubqueries.get(colIdx);
        if (mat) {
          const key = db._resolveColumn(mat.outerCol, row);
          result[name] = mat.lookup.get(key) ?? mat.defaultValue;
        } else {
          const subResult = db._evalSubquery(col.subquery, row);
          result[name] = subResult.length > 0 ? Object.values(subResult[0])[0] : null;
        }
      } else if (col.type === 'window') {
        const name = col.alias || `${col.func}(${col.arg || ''})`;
        result[name] = row[`__window_${name}`];
      } else if (col.name) {
        // Strip table prefix from output name (c1.email → email)
        const colName = String(col.name);
        const baseName = colName.includes('.') ? colName.split('.').pop() : colName;
        const name = col.alias || baseName;
        result[name] = db._resolveColumn(colName, row);
      } else if (col.type === 'qualified_star') {
        // Expand table.* — add all columns from that table
        const prefix = col.table + '.';
        for (const [key, val] of Object.entries(row)) {
          if (key.startsWith(prefix)) {
            result[key.slice(prefix.length)] = val;
          } else if (!key.includes('.')) {
            // In single-table or self-join context, include unqualified columns
            // only if no other table has claimed them
            // (we'll skip this for now — prefer qualified matches)
          }
        }
        // If no qualified matches found, try matching via table schema
        if (Object.keys(result).length === 0 || !Object.keys(result).some(k => k !== undefined)) {
          const table = db.tables.get(col.table);
          if (table) {
            for (const s of table.schema) {
              const val = row[prefix + s.name] ?? row[s.name];
              if (val !== undefined) result[s.name] = val;
            }
          }
        }
      }
    }
    return result;
  });

  // DISTINCT / DISTINCT ON
  let finalRows = projected;
  if (ast.distinctOn) {
    // DISTINCT ON: keep first row per unique combination of ON expressions
    // Uses pre-ORDER BY rows for key evaluation, then filters projected
    const seen = new Set();
    finalRows = [];
    for (let i = 0; i < projected.length; i++) {
      const row = rows[i]; // use pre-projection row for expression evaluation
      const key = ast.distinctOn.map(expr => JSON.stringify(db._evalValue(expr, row))).join('|');
      if (!seen.has(key)) {
        seen.add(key);
        finalRows.push(projected[i]);
      }
    }
    if (ast.offset) finalRows = finalRows.slice(Math.max(0, ast.offset));
    if (ast.limit != null && ast.limit >= 0) finalRows = finalRows.slice(0, ast.limit);
  } else if (ast.distinct) {
    const seen = new Set();
    finalRows = projected.filter(row => {
      const key = JSON.stringify(row);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    // Apply OFFSET and LIMIT after DISTINCT
    if (ast.offset) finalRows = finalRows.slice(Math.max(0, ast.offset));
    if (ast.limit != null && ast.limit >= 0) finalRows = finalRows.slice(0, ast.limit);
  }

  return { type: 'ROWS', rows: finalRows };
}

/**
 * Try to execute a GROUP BY query via the vectorized engine.
 * Returns null if the query isn't eligible (falls through to standard path).
 * Only handles simple cases: no HAVING, no window functions, no GROUPING SETS,
 * no alias references in GROUP BY (must reference actual column names).
 */
function _tryVectorizedGroupBy(db, ast, rows) {
  // Skip if query has features the vectorized path doesn't handle well
  if (ast.having) return null;
  if (ast.groupingSets || ast.rollup || ast.cube) return null;
  if (!Array.isArray(ast.groupBy)) return null; // ROLLUP/CUBE/GROUPING SETS
  if (db._columnsHaveWindow(ast.columns)) return null;
  if (!rows.length) return null;
  
  // Skip if any aggregate has FILTER clause (vectorized doesn't support it)
  for (const col of ast.columns) {
    if (col.type === 'aggregate' && col.filter) return null;
  }
  
  // Skip if any column is a CASE expression, function-wrapped aggregate, or expression
  // The vectorized path only handles raw columns and simple aggregates
  for (const col of ast.columns) {
    if (col.type !== 'column' && col.type !== 'aggregate') return null;
  }
  
  // Check if the query shape is eligible
  if (!canVectorize(ast)) return null;
  
  // Verify GROUP BY columns are actual columns in the row data (not aliases)
  const sampleRow = rows[0];
  for (const groupCol of ast.groupBy) {
    if (typeof groupCol !== 'string') return null;
    if (!(groupCol in sampleRow)) return null; // alias or expression — bail
  }
  
  // Verify aggregate source columns exist in row data
  for (const col of ast.columns) {
    if (col.type === 'aggregate' && col.arg !== '*' && typeof col.arg === 'string') {
      if (!(col.arg in sampleRow)) return null;
    }
  }
  
  try {
    let resultRows = vectorizedGroupBy(rows, ast);
    
    // Apply ORDER BY if present
    if (ast.orderBy) {
      resultRows.sort((a, b) => {
        for (const { column, direction } of ast.orderBy) {
          let av, bv;
          if (typeof column === 'string') {
            av = a[column];
            bv = b[column];
            // Try resolving via db if not found directly
            if (av === undefined) av = db._orderByValue(column, a);
            if (bv === undefined) bv = db._orderByValue(column, b);
          } else if (typeof column === 'number') {
            const selCol = ast.columns[column - 1];
            const name = selCol?.alias || selCol?.name;
            if (name) { av = a[name]; bv = b[name]; }
          } else {
            av = db._evalValue(column, a);
            bv = db._evalValue(column, b);
          }
          if (av == null && bv == null) continue;
          if (av == null) return direction === 'DESC' ? -1 : 1;
          if (bv == null) return direction === 'DESC' ? 1 : -1;
          if (av < bv) return direction === 'DESC' ? 1 : -1;
          if (av > bv) return direction === 'DESC' ? -1 : 1;
        }
        return 0;
      });
    }
    
    // Apply LIMIT/OFFSET
    if (ast.offset != null) resultRows = resultRows.slice(ast.offset);
    if (ast.limit != null && ast.limit >= 0) resultRows = resultRows.slice(0, ast.limit);
    
    return { type: 'ROWS', rows: resultRows };
  } catch {
    // Vectorized failed — fall through to standard path
    return null;
  }
}

