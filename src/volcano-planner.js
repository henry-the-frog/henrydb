// volcano-planner.js — Converts SQL AST to volcano iterator tree
// Bridges the SQL parser to the volcano execution engine

import {
  Iterator, SeqScan, ValuesIter, Filter, Project, Limit, Distinct, Union,
  NestedLoopJoin, HashJoin, MergeJoin, Sort, HashAggregate, IndexNestedLoopJoin,
  CTE, RecursiveCTE,
} from './volcano.js';
import {
  estimateSelectivity, estimateCardinality, chooseBestJoin, shouldUseIndexScan,
} from './volcano-cost.js';
import { applyWindowFunctions } from './window-functions.js';

/**
 * Build a plan and return the EXPLAIN output string.
 */
export function explainPlan(ast, tables, indexCatalog) {
  const plan = buildPlan(ast, tables, indexCatalog);
  return plan.explain();
}

/**
 * Build a volcano iterator tree from a SELECT AST and database tables.
 * @param {object} ast — parsed SELECT AST
 * @param {Map} tables — database tables map (name → { heap, schema, indexes })
 * @param {Map} [indexCatalog] — optional index catalog for INL join selection
 */
export function buildPlan(ast, tables, indexCatalog) {
  // Handle UNION/INTERSECT/EXCEPT at the top level
  if (ast.type === 'UNION') {
    const leftPlan = buildPlan(ast.left, tables, indexCatalog);
    const rightPlan = buildPlan(ast.right, tables, indexCatalog);
    let iter = new Union(leftPlan, rightPlan);
    if (!ast.all) {
      // For UNION dedup, normalize rows first (strip qualified names + internals)
      iter = wrapNormalizeForSetOp(iter, tables, ast.left);
      iter = new Distinct(iter);
    }
    // ORDER BY on combined result
    if (ast.orderBy && ast.orderBy.length > 0) {
      const orderSpec = ast.orderBy.map(o => ({
        column: o.column,
        desc: o.direction === 'DESC',
      }));
      iter = new Sort(iter, orderSpec);
    }
    // LIMIT/OFFSET
    if (ast.limit != null || ast.offset != null) {
      iter = new Limit(iter, ast.limit, ast.offset || 0);
    }
    return iter;
  }
  
  if (ast.type === 'INTERSECT') {
    const leftPlan = buildPlan(ast.left, tables, indexCatalog);
    const rightPlan = buildPlan(ast.right, tables, indexCatalog);
    
    const leftRows = materialize(leftPlan);
    const rightRows = materialize(rightPlan);
    const leftNorm = leftRows.map(normalizeSetRow);
    const rightNorm = rightRows.map(normalizeSetRow);
    
    if (ast.all) {
      const rightCounts = new Map();
      for (const r of rightNorm) {
        const key = JSON.stringify(r);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const result = [];
      for (let i = 0; i < leftNorm.length; i++) {
        const key = JSON.stringify(leftNorm[i]);
        const count = rightCounts.get(key) || 0;
        if (count > 0) {
          result.push(leftNorm[i]);
          rightCounts.set(key, count - 1);
        }
      }
      return new ValuesIter(result);
    }
    
    const rightKeys = new Set(rightNorm.map(r => JSON.stringify(r)));
    const seen = new Set();
    const result = [];
    for (const r of leftNorm) {
      const key = JSON.stringify(r);
      if (rightKeys.has(key) && !seen.has(key)) {
        seen.add(key);
        result.push(r);
      }
    }
    return new ValuesIter(result);
  }
  
  if (ast.type === 'EXCEPT') {
    const leftPlan = buildPlan(ast.left, tables, indexCatalog);
    const rightPlan = buildPlan(ast.right, tables, indexCatalog);
    
    const leftRows = materialize(leftPlan);
    const rightRows = materialize(rightPlan);
    const leftNorm = leftRows.map(normalizeSetRow);
    const rightNorm = rightRows.map(normalizeSetRow);
    
    if (ast.all) {
      const rightCounts = new Map();
      for (const r of rightNorm) {
        const key = JSON.stringify(r);
        rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
      }
      const result = [];
      for (const r of leftNorm) {
        const key = JSON.stringify(r);
        const count = rightCounts.get(key) || 0;
        if (count > 0) {
          rightCounts.set(key, count - 1);
        } else {
          result.push(r);
        }
      }
      return new ValuesIter(result);
    }
    
    const rightKeys = new Set(rightNorm.map(r => JSON.stringify(r)));
    const seen = new Set();
    const result = [];
    for (const r of leftNorm) {
      const key = JSON.stringify(r);
      if (!rightKeys.has(key) && !seen.has(key)) {
        seen.add(key);
        result.push(r);
      }
    }
    return new ValuesIter(result);
  }

  // 0. Handle CTEs — materialize subqueries and add as virtual tables
  let effectiveTables = tables;
  if (ast.ctes && ast.ctes.length > 0) {
    effectiveTables = new Map(tables);
    for (const cte of ast.ctes) {
      if (cte.recursive) {
        // Recursive CTE: iteratively materialize until fixed point
        const unionAst = cte.query; // type: 'UNION'
        const baseAst = unionAst.left;
        const recursiveAst = unionAst.right;
        const columnList = cte.columnList;
        
        // Build and execute base case
        const basePlan = buildPlan(baseAst, effectiveTables, indexCatalog);
        basePlan.open();
        let allRows = [];
        let row;
        while ((row = basePlan.next()) !== null) {
          allRows.push(row);
        }
        basePlan.close();
        
        // Rename columns if columnList provided
        if (columnList && allRows.length > 0) {
          const origColumns = Object.keys(allRows[0]);
          allRows = allRows.map(r => {
            const renamed = {};
            for (let i = 0; i < columnList.length && i < origColumns.length; i++) {
              renamed[columnList[i]] = r[origColumns[i]];
            }
            return renamed;
          });
        }
        
        // Iterate recursive step until fixed point
        let workingTable = [...allRows];
        const maxDepth = 100;
        let depth = 0;
        
        while (workingTable.length > 0 && depth < maxDepth) {
          // Create virtual table for self-reference
          const columns = columnList || Object.keys(workingTable[0] || {});
          const tempTables = new Map(effectiveTables);
          tempTables.set(cte.name, {
            heap: workingTable,
            schema: columns.map(c => ({ name: c, type: 'ANY' })),
            _cteMaterialized: true,
          });
          
          const stepPlan = buildPlan(recursiveAst, tempTables, indexCatalog);
          stepPlan.open();
          const newRows = [];
          while ((row = stepPlan.next()) !== null) {
            // Rename to match column list
            if (columnList) {
              const origCols = Object.keys(row);
              const renamed = {};
              for (let i = 0; i < columnList.length && i < origCols.length; i++) {
                renamed[columnList[i]] = row[origCols[i]];
              }
              newRows.push(renamed);
            } else {
              newRows.push(row);
            }
          }
          stepPlan.close();
          
          if (newRows.length === 0) break;
          allRows.push(...newRows);
          workingTable = newRows;
          depth++;
        }
        
        const columns = columnList || (allRows.length > 0 ? Object.keys(allRows[0]) : []);
        effectiveTables.set(cte.name, {
          heap: allRows,
          schema: columns.map(c => ({ name: c, type: 'ANY' })),
          _cteMaterialized: true,
        });
      } else {
        // Non-recursive CTE: build plan, materialize eagerly
        const ctePlan = buildPlan(cte.query, effectiveTables, indexCatalog);
        
        // Materialize the CTE results
        ctePlan.open();
        const rows = [];
        let row;
        while ((row = ctePlan.next()) !== null) {
          rows.push(row);
        }
        ctePlan.close();
        
        // Apply column rename if columnList specified
        let materializedRows = rows;
        if (cte.columnList && rows.length > 0) {
          const origColumns = Object.keys(rows[0]);
          materializedRows = rows.map(r => {
            const renamed = {};
            for (let i = 0; i < cte.columnList.length && i < origColumns.length; i++) {
              renamed[cte.columnList[i]] = r[origColumns[i]];
            }
            return renamed;
          });
        }
        
        const columns = cte.columnList || (materializedRows.length > 0 ? Object.keys(materializedRows[0]) : []);
        effectiveTables.set(cte.name, {
          heap: materializedRows,
          schema: columns.map(c => ({ name: c, type: 'ANY' })),
          _cteMaterialized: true,
        });
      }
    }
  }

  // 1. Build scan for FROM table
  let iter = buildScanNode(ast.from, effectiveTables);

  // 2. Build JOIN nodes
  if (ast.joins && ast.joins.length > 0) {
    for (const join of ast.joins) {
      const rightTableName = typeof join.table === 'string' ? join.table : join.table;
      const rightAlias = join.alias || rightTableName;
      const rightIter = buildScanNode({ table: rightTableName, alias: rightAlias }, effectiveTables);
      const predicate = join.on ? buildPredicate(join.on) : null;
      
      // Choose join strategy:
      // 1. IndexNestedLoopJoin if inner table has usable index on join key
      // 2. HashJoin for equi-joins
      // 3. NestedLoopJoin as fallback
      const equiJoin = extractEquiJoinKeys(join.on, ast.from, join);
      
      if (equiJoin && indexCatalog && join.joinType !== 'LEFT') {
        const inlJoin = tryIndexNestedLoopJoin(
          iter, equiJoin, ast.from, join, effectiveTables, indexCatalog
        );
        if (inlJoin) {
          iter = inlJoin;
          continue;
        }
      }
      
      if (equiJoin) {
        // Cost-based join strategy selection
        const leftTableName = typeof ast.from === 'string' ? ast.from : ast.from.table;
        const rightTableName2 = join.table?.table || join.table;
        const leftCard = estimateCardinality(effectiveTables, leftTableName, ast.where);
        const rightCard = estimateCardinality(effectiveTables, rightTableName2, null);
        const leftSorted = isSortedOn(iter, equiJoin.probeKey);
        const rightSorted = isSortedOn(rightIter, equiJoin.buildKey);
        
        const strategy = chooseBestJoin(leftCard, rightCard, leftSorted, rightSorted);
        
        if (strategy === 'merge' && leftSorted && rightSorted) {
          iter = new MergeJoin(iter, rightIter, equiJoin.probeKey, equiJoin.buildKey);
        } else if (strategy === 'merge') {
          // Need to sort both sides first
          const leftSort = new Sort(iter, [{ column: equiJoin.probeKey, desc: false }]);
          const rightSort = new Sort(rightIter, [{ column: equiJoin.buildKey, desc: false }]);
          iter = new MergeJoin(leftSort, rightSort, equiJoin.probeKey, equiJoin.buildKey);
        } else if (strategy === 'nested_loop') {
          iter = new NestedLoopJoin(iter, rightIter, predicate);
        } else {
          // Default: hash join
          iter = new HashJoin(rightIter, iter, equiJoin.buildKey, equiJoin.probeKey,
            join.joinType === 'LEFT' ? 'left' : 'inner');
        }
      } else {
        iter = new NestedLoopJoin(iter, rightIter, predicate ? (l, r) => predicate({ ...l, ...r }) : null,
          join.joinType === 'LEFT' ? 'left' : 'inner');
      }
    }
  }

  // 3. WHERE filter
  if (ast.where) {
    const pred = buildPredicate(ast.where);
    iter = new Filter(iter, pred);
  }

  // 4. GROUP BY + aggregates
  const hasAggregates = ast.columns.some(c => c.type === 'aggregate');
  if (ast.groupBy || hasAggregates) {
    const groupBy = ast.groupBy || [];
    const aggregates = [];
    
    for (const col of ast.columns) {
      if (col.type === 'aggregate') {
        const name = col.alias || `${col.func}(${col.arg})`;
        aggregates.push({ name, func: col.func, column: col.arg });
      }
    }
    
    iter = new HashAggregate(iter, groupBy, aggregates);

    // 5. HAVING filter (applied after aggregation)
    if (ast.having) {
      const havingPred = buildAggregatePredicate(ast.having, ast.columns);
      iter = new Filter(iter, havingPred);
    }
  }

  // 6. Window functions (BEFORE projection to preserve columns used in OVER)
  const windowCols = (ast.columns || []).filter(c => c.type === 'window');
  if (windowCols.length > 0) {
    const rows = materialize(iter);
    const specs = windowCols.map(col => ({
      func: col.func,
      args: col.arg ? [col.arg] : [],
      partitionBy: col.over?.partitionBy?.map(p => typeof p === 'string' ? p : p.column || p.name) || [],
      orderBy: col.over?.orderBy?.map(o => ({
        column: typeof o.column === 'string' ? o.column : (o.column?.name || o.column),
        direction: o.direction || 'ASC',
      })) || [],
      alias: col.alias || col.func,
    }));
    const withWindows = applyWindowFunctions(rows, specs);
    iter = new ValuesIter(withWindows);
  }

  // 7. SELECT projection
  const projections = buildProjections(ast.columns, hasAggregates);
  if (projections && !hasAggregates) {
    iter = new Project(iter, projections);
  } else if (hasAggregates) {
    iter = new Project(iter, buildAggregateProjections(ast.columns));
  }

  // 8. DISTINCT
  if (ast.distinct) {
    iter = new Distinct(iter);
  }

  // 8. ORDER BY
  if (ast.orderBy && ast.orderBy.length > 0) {
    const orderSpec = ast.orderBy.map(o => ({
      column: resolveOutputColumn(o.column, ast.columns),
      desc: o.direction === 'DESC',
    }));
    iter = new Sort(iter, orderSpec);
  }

  // 9. LIMIT / OFFSET
  if (ast.limit != null) {
    iter = new Limit(iter, ast.limit, ast.offset || 0);
  }

  return iter;
}

// --- Helpers ---

/**
 * Normalize a row for set operations: strip qualified names (x.col),
 * internal fields (_pageId, _slotIdx), and keep only unqualified columns.
 */
function normalizeSetRow(row) {
  const result = {};
  for (const [k, v] of Object.entries(row)) {
    if (k.startsWith('_') || k.includes('.')) continue;
    result[k] = v;
  }
  return result;
}

/**
 * Materialize an iterator into an array of rows.
 */
function materialize(iter) {
  iter.open();
  const rows = [];
  let row;
  while ((row = iter.next()) !== null) rows.push(row);
  iter.close();
  return rows;
}

/**
 * Wrap a Union iterator to normalize rows for UNION dedup.
 * Returns a new iterator that strips qualified/internal columns.
 */
function wrapNormalizeForSetOp(iter, tables, leftAst) {
  return new NormalizeIterator(iter);
}

class NormalizeIterator extends Iterator {
  constructor(child) {
    super();
    this._child = child;
  }
  open() { this._child.open(); }
  next() {
    const row = this._child.next();
    if (row === null) return null;
    return normalizeSetRow(row);
  }
  close() { this._child.close(); }
  describe() {
    return { type: 'Normalize', children: [this._child], details: {} };
  }
}

/**
 * Check if an iterator is already sorted on a given column.
 * Detects Sort nodes whose primary key matches the column.
 */
function isSortedOn(iter, column) {
  if (iter instanceof Sort) {
    const orderBy = iter._orderBy;
    if (orderBy && orderBy.length > 0) {
      const primaryKey = orderBy[0].column;
      // Match column name (with or without alias prefix)
      if (primaryKey === column || primaryKey.endsWith('.' + column) || column.endsWith('.' + primaryKey)) return true;
    }
  }
  return false;
}

function buildScanNode(fromClause, tables) {
  if (!fromClause) return new ValuesIter([{}]); // Dummy for SELECT without FROM
  
  const tableName = typeof fromClause === 'string' ? fromClause : fromClause.table;
  const alias = typeof fromClause === 'string' ? null : fromClause.alias;
  
  const tableObj = tables.get(tableName) || tables.get(tableName.toLowerCase());
  if (!tableObj) throw new Error(`Table ${tableName} not found`);

  // CTE with materialized rows — scan from array with alias qualification
  if (tableObj._cteMaterialized && Array.isArray(tableObj.heap)) {
    const qualifyAlias = alias || tableName;
    const rows = tableObj.heap;
    // Add alias-qualified column names like SeqScan does
    const qualifiedRows = rows.map(r => {
      const qr = { ...r };
      for (const [k, v] of Object.entries(r)) {
        qr[`${qualifyAlias}.${k}`] = v;
      }
      return qr;
    });
    return new ValuesIter(qualifiedRows);
  }

  const columns = tableObj.schema.map(c => c.name);
  return new SeqScan(tableObj.heap, columns, alias || tableName);
}

function buildPredicate(expr) {
  if (!expr) return () => true;

  switch (expr.type) {
    case 'COMPARE': {
      const getLeft = buildValueGetter(expr.left);
      const getRight = buildValueGetter(expr.right);
      const cmp = comparators[expr.op];
      if (!cmp) throw new Error(`Unknown comparison: ${expr.op}`);
      return (row) => cmp(getLeft(row), getRight(row));
    }
    case 'AND': {
      const left = buildPredicate(expr.left);
      const right = buildPredicate(expr.right);
      return (row) => left(row) && right(row);
    }
    case 'OR': {
      const left = buildPredicate(expr.left);
      const right = buildPredicate(expr.right);
      return (row) => left(row) || right(row);
    }
    case 'NOT': {
      const inner = buildPredicate(expr.operand || expr.expr);
      return (row) => !inner(row);
    }
    case 'IS_NULL': {
      const getter = buildValueGetter(expr.column || expr.expr);
      return (row) => getter(row) == null;
    }
    case 'IS_NOT_NULL': {
      const getter = buildValueGetter(expr.column || expr.expr);
      return (row) => getter(row) != null;
    }
    case 'BETWEEN': {
      const val = buildValueGetter(expr.expr);
      const low = buildValueGetter(expr.low);
      const high = buildValueGetter(expr.high);
      return (row) => { const v = val(row); return v >= low(row) && v <= high(row); };
    }
    case 'IN': {
      const val = buildValueGetter(expr.expr);
      const vals = expr.values.map(buildValueGetter);
      return (row) => { const v = val(row); return vals.some(g => g(row) === v); };
    }
    case 'LIKE': {
      const val = buildValueGetter(expr.expr);
      const pattern = expr.pattern;
      const regex = new RegExp('^' + pattern.replace(/%/g, '.*').replace(/_/g, '.') + '$', 'i');
      return (row) => regex.test(val(row));
    }
    default:
      return () => true;
  }
}

function buildValueGetter(expr) {
  if (!expr) return () => null;
  
  switch (expr.type) {
    case 'literal':
      return () => expr.value;
    case 'column_ref':
    case 'column':
      return (row) => {
        const name = expr.name;
        if (name in row) return row[name];
        // Try lowercase
        const lower = name.toLowerCase();
        if (lower in row) return row[lower];
        // Try without alias prefix
        for (const [k, v] of Object.entries(row)) {
          if (k.endsWith('.' + name) || k.toLowerCase() === lower) return v;
        }
        return row[name]; // undefined
      };
    case 'binary_expr':
    case 'arith': {
      const left = buildValueGetter(expr.left);
      const right = buildValueGetter(expr.right);
      const op = expr.op;
      return (row) => {
        const l = left(row), r = right(row);
        switch (op) {
          case '+': return l + r;
          case '-': return l - r;
          case '*': return l * r;
          case '/': return r !== 0 ? l / r : null;
          case '%': return l % r;
          default: return null;
        }
      };
    }
    case 'function_call': {
      const args = (expr.args || []).map(buildValueGetter);
      return (row) => {
        const vals = args.map(g => g(row));
        switch (expr.name.toUpperCase()) {
          case 'ABS': return Math.abs(vals[0]);
          case 'UPPER': return String(vals[0]).toUpperCase();
          case 'LOWER': return String(vals[0]).toLowerCase();
          case 'LENGTH': return String(vals[0]).length;
          case 'COALESCE': return vals.find(v => v != null) ?? null;
          default: return null;
        }
      };
    }
    default:
      return () => null;
  }
}

const comparators = {
  EQ: (a, b) => a === b,
  NE: (a, b) => a !== b,
  NEQ: (a, b) => a !== b,
  LT: (a, b) => a < b,
  LE: (a, b) => a <= b,
  GT: (a, b) => a > b,
  GE: (a, b) => a >= b,
};

/**
 * Try to build an IndexNestedLoopJoin if the inner table has a usable index.
 * Returns the INL join iterator, or null if no usable index found.
 */
function tryIndexNestedLoopJoin(outerIter, equiJoin, leftFrom, rightJoin, tables, indexCatalog) {
  // Determine which side is outer (already built = left) and inner (right = join table)
  const rightTableName = typeof rightJoin.table === 'string' ? rightJoin.table : rightJoin.table;
  const rightAlias = rightJoin.alias || rightTableName;
  
  const rightTable = tables.get(rightTableName) || tables.get(rightTableName.toLowerCase());
  if (!rightTable || !rightTable.indexes) return null;
  
  // Find the join column on the right (inner) table
  // equiJoin.buildKey might be "alias.column" or just "column"
  const rightJoinCol = resolveTableColumn(equiJoin.buildKey, rightAlias);
  if (!rightJoinCol) return null;
  
  // Check if the right table has an index on this column
  const idx = rightTable.indexes.get(rightJoinCol);
  if (!idx) return null;
  
  // Build INL join: outer drives, inner probes index
  const innerColumns = rightTable.schema.map(c => c.name);
  
  return new IndexNestedLoopJoin(
    outerIter,
    idx,
    rightTable.heap,
    innerColumns,
    equiJoin.probeKey, // Outer key to look up
    rightAlias,
  );
}

/**
 * Resolve a potentially qualified column name (e.g., "d.id") to just the column name,
 * verifying it belongs to the given table alias.
 */
function resolveTableColumn(qualifiedName, alias) {
  if (qualifiedName.includes('.')) {
    const [table, col] = qualifiedName.split('.');
    if (table === alias) return col;
    return null; // Wrong table
  }
  return qualifiedName;
}

function extractEquiJoinKeys(on, leftFrom, rightJoin) {
  if (!on || on.type !== 'COMPARE' || on.op !== 'EQ') return null;
  if (on.left.type !== 'column_ref' || on.right.type !== 'column_ref') return null;
  
  // Figure out which column belongs to which table
  const leftAlias = leftFrom.alias || leftFrom.table || leftFrom;
  const rightAlias = rightJoin.alias || rightJoin.table;
  
  const lName = on.left.name;
  const rName = on.right.name;
  
  // Check if names are qualified (e.g., "u.id")
  if (lName.includes('.') && rName.includes('.')) {
    const [lTable, lCol] = lName.split('.');
    const [rTable, rCol] = rName.split('.');
    // build side = right table (smaller side), probe = left (already built)
    return { buildKey: rName, probeKey: lName };
  }
  
  // Unqualified — just use the names directly
  return { buildKey: rName, probeKey: lName };
}

function buildProjections(columns, hasAggregates) {
  if (!columns || columns.length === 0) return null;
  if (columns.length === 1 && columns[0].type === 'star') return null; // SELECT *
  if (hasAggregates) return null; // Handled separately
  
  return columns.map(col => {
    const outputName = col.alias || col.name;
    switch (col.type) {
      case 'column':
        return { name: outputName, expr: buildValueGetter({ type: 'column_ref', name: col.name }) };
      case 'star':
        return null; // Can't project * — passthrough
      case 'expression':
        return { name: outputName, expr: buildValueGetter(col.expr) };
      case 'window':
        // Window function value was already computed and stored under the alias
        return { name: outputName, expr: (row) => row[outputName] };
      default:
        return { name: outputName, expr: () => null };
    }
  }).filter(Boolean);
}

function buildAggregateProjections(columns) {
  return columns.map(col => {
    if (col.type === 'aggregate') {
      const name = col.alias || `${col.func}(${col.arg})`;
      return { name: col.alias || name, expr: (row) => row[name] };
    }
    if (col.type === 'column') {
      const name = col.alias || col.name;
      return { name, expr: (row) => row[col.name] ?? row[name] };
    }
    return { name: col.alias || 'expr', expr: () => null };
  });
}

function buildAggregatePredicate(expr, columns) {
  // HAVING uses aggregate expressions — resolve to their output names
  if (expr.type === 'COMPARE') {
    const getLeft = buildAggregateValueGetter(expr.left, columns);
    const getRight = buildAggregateValueGetter(expr.right, columns);
    const cmp = comparators[expr.op];
    return (row) => cmp(getLeft(row), getRight(row));
  }
  return () => true;
}

function buildAggregateValueGetter(expr, columns) {
  if (expr.type === 'literal') return () => expr.value;
  if (expr.type === 'aggregate_expr') {
    // Find the matching aggregate column's output name
    const match = columns.find(c => c.type === 'aggregate' && c.func === expr.func && c.arg === expr.arg);
    const name = match ? (match.alias || `${match.func}(${match.arg})`) : `${expr.func}(${expr.arg})`;
    return (row) => row[name];
  }
  return buildValueGetter(expr);
}

function resolveOutputColumn(name, columns) {
  // Order by might reference an alias
  for (const col of columns) {
    if (col.alias === name) {
      if (col.type === 'aggregate') return col.alias || `${col.func}(${col.arg})`;
      return col.name;
    }
  }
  return name;
}
