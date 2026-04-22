// volcano-planner.js — Converts SQL AST to volcano iterator tree
// Bridges the SQL parser to the volcano execution engine

import {
  SeqScan, ValuesIter, Filter, Project, Limit, Distinct,
  NestedLoopJoin, HashJoin, Sort, HashAggregate, IndexNestedLoopJoin,
  IndexScan, Union, CTE as CTEIterator,
} from './volcano.js';

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
  // Handle WITH (CTE)
  if (ast.type === 'WITH') {
    // Materialize each CTE as a temporary table
    const cteTables = new Map(tables);
    for (const cte of (ast.ctes || [])) {
      const ctePlan = buildPlan(cte.query, cteTables, indexCatalog);
      // Materialize: execute the CTE plan and store results
      ctePlan.open();
      const rows = [];
      let row;
      while ((row = ctePlan.next()) !== null) rows.push(row);
      ctePlan.close();
      
      // Create a virtual table for the CTE
      const schema = rows.length > 0 ? Object.keys(rows[0]).map(name => ({ name })) : [];
      cteTables.set(cte.name, {
        heap: { scan: function*() { for (const r of rows) yield { values: schema.map(c => r[c.name]), pageId: 0, slotIdx: 0 }; } },
        schema
      });
    }
    // Build the main query plan with CTE tables available
    return buildPlan(ast.query, cteTables, indexCatalog);
  }

  // Handle UNION/UNION ALL
  if (ast.type === 'UNION') {
    const leftPlan = buildPlan(ast.left, tables, indexCatalog);
    const rightPlan = buildPlan(ast.right, tables, indexCatalog);
    let iter = new Union(leftPlan, rightPlan);
    if (!ast.all) {
      iter = new Distinct(iter);
    }
    return iter;
  }

  // 1. Build scan for FROM table
  let iter = buildScanNode(ast.from, tables);

  // Predicate pushdown: split WHERE into per-table conditions
  let residualWhere = ast.where;
  if (ast.where && ast.joins && ast.joins.length > 0) {
    const fromTableName = ast.from.alias || ast.from.table;
    const joinTableNames = ast.joins.map(j => j.alias || (typeof j.table === 'string' ? j.table : j.table));
    const allTables = [fromTableName, ...joinTableNames];
    
    const { perTable, residual } = splitWhereByTable(ast.where, allTables);
    
    // Push FROM table predicates into a Filter on the FROM scan
    if (perTable[fromTableName]) {
      const pred = buildPredicate(perTable[fromTableName]);
      iter = new Filter(iter, pred);
    }
    
    residualWhere = residual;
    
    // Store per-table predicates for join table pushdown
    var _pushdownPredicates = perTable;
  }

  // 2. Build JOIN nodes
  if (ast.joins && ast.joins.length > 0) {
    for (const join of ast.joins) {
      const rightTableName = typeof join.table === 'string' ? join.table : join.table;
      const rightAlias = join.alias || rightTableName;
      let rightIter = buildScanNode({ table: rightTableName, alias: rightAlias }, tables);
      
      // Push per-table predicate into right scan
      if (_pushdownPredicates && _pushdownPredicates[rightAlias]) {
        rightIter = new Filter(rightIter, buildPredicate(_pushdownPredicates[rightAlias]));
      }
      
      const predicate = join.on ? buildPredicate(join.on) : null;
      
      // Choose join strategy:
      // 1. IndexNestedLoopJoin if inner table has usable index on join key
      // 2. HashJoin for equi-joins
      // 3. NestedLoopJoin as fallback
      const equiJoin = extractEquiJoinKeys(join.on, ast.from, join);
      
      if (equiJoin && indexCatalog) {
        const inlJoin = tryIndexNestedLoopJoin(
          iter, equiJoin, ast.from, join, tables, indexCatalog
        );
        if (inlJoin) {
          iter = inlJoin;
          continue;
        }
      }
      
      if (equiJoin) {
        const jt = join.joinType === 'LEFT' ? 'left' : join.joinType === 'RIGHT' ? 'right' : join.joinType === 'FULL' ? 'full' : 'inner';
        iter = new HashJoin(rightIter, iter, equiJoin.buildKey, equiJoin.probeKey, jt);
      } else {
        const jt = join.joinType === 'LEFT' ? 'left' : join.joinType === 'RIGHT' ? 'right' : join.joinType === 'FULL' ? 'full' : 'inner';
        iter = new NestedLoopJoin(iter, rightIter, predicate ? (l, r) => predicate({ ...l, ...r }) : null, jt);
      }
    }
  }

  // 3. WHERE filter — apply remaining (cross-table) predicates
  const effectiveWhere = residualWhere !== undefined ? residualWhere : ast.where;
  if (effectiveWhere) {
    const indexScanResult = tryIndexScan(effectiveWhere, ast.from, tables, indexCatalog);
    if (indexScanResult) {
      // Replace SeqScan with IndexScan and apply remaining predicates
      iter = indexScanResult.scan;
      if (indexScanResult.residual) {
        iter = new Filter(iter, buildPredicate(indexScanResult.residual));
      }
    } else {
      const pred = buildPredicate(effectiveWhere);
      iter = new Filter(iter, pred);
    }
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

  // 6. SELECT projection
  const projections = buildProjections(ast.columns, hasAggregates);
  if (projections && !hasAggregates) {
    // Don't project after aggregate — column names already set by HashAggregate
    iter = new Project(iter, projections);
  } else if (hasAggregates) {
    // For aggregates, project to rename/select the right columns
    iter = new Project(iter, buildAggregateProjections(ast.columns));
  }

  // 7. DISTINCT
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
 * Try to use an IndexScan for a WHERE predicate.
 * Looks for equality (col = val) or range (col BETWEEN low AND high) on indexed columns.
 * Returns { scan: IndexScan, residual: remainingWhere } or null.
 */
function tryIndexScan(where, fromClause, tables, indexCatalog) {
  if (!where || !fromClause) return null;
  
  const tableName = typeof fromClause === 'string' ? fromClause : fromClause.table;
  const alias = typeof fromClause === 'string' ? null : fromClause.alias;
  const tableObj = tables.get(tableName) || tables.get(tableName?.toLowerCase());
  if (!tableObj) return null;
  
  const columns = tableObj.schema.map(c => c.name);
  const totalRows = tableObj.heap?.rowCount || tableObj.heap?.tupleCount || 0;
  
  // Cost-based selectivity threshold: skip index if scanning > 30% of table
  const INDEX_SELECTIVITY_THRESHOLD = 0.30;
  
  // Check for equality condition: col = literal
  if (where.type === 'COMPARE' && where.op === 'EQ') {
    const { colName, value } = extractColLiteral(where, alias || tableName);
    if (colName && value !== undefined) {
      const index = findIndex(tableObj, colName, indexCatalog, tableName);
      if (index) {
        // Equality on index is almost always worth it (selectivity ~ 1/ndv)
        return {
          scan: new IndexScan(index, tableObj.heap, columns, value, value, alias || tableName),
          residual: null
        };
      }
    }
  }
  
  // Check for range conditions: col > val, col >= val, col < val, col <= val
  if (where.type === 'COMPARE' && ['LT', 'LE', 'GT', 'GE'].includes(where.op)) {
    const { colName, value, colSide } = extractColLiteral(where, alias || tableName);
    if (colName && value !== undefined) {
      const index = findIndex(tableObj, colName, indexCatalog, tableName);
      if (index) {
        // Estimate range selectivity: default 33% for range queries
        // For small tables (< 50 rows), SeqScan is almost always faster
        const estimatedSelectivity = 0.33;
        if (totalRows < 50 || estimatedSelectivity > INDEX_SELECTIVITY_THRESHOLD) {
          // Skip: prefer SeqScan for small tables or wide ranges
          // Exception: if table is large enough that even 33% is many random I/Os
          if (totalRows < 500) {
            return null; // SeqScan wins for small-medium tables with range predicates
          }
        }
        
        let low, high;
        if (colSide === 'left') {
          if (where.op === 'GT' || where.op === 'GE') low = value;
          if (where.op === 'LT' || where.op === 'LE') high = value;
        } else {
          if (where.op === 'GT' || where.op === 'GE') high = value;
          if (where.op === 'LT' || where.op === 'LE') low = value;
        }
        return {
          scan: new IndexScan(index, tableObj.heap, columns, low, high, alias || tableName),
          residual: where // Keep full predicate as residual for exact filtering
        };
      }
    }
  }
  
  // Check for BETWEEN: col BETWEEN low AND high
  if (where.type === 'BETWEEN') {
    const colNode = where.expr || where.left;
    const lowNode = where.low;
    const highNode = where.high;
    if (colNode?.type === 'column_ref' && lowNode?.type === 'literal' && highNode?.type === 'literal') {
      const colName = colNode.name?.includes('.') ? colNode.name.split('.').pop() : colNode.name;
      const index = findIndex(tableObj, colName, indexCatalog, tableName);
      if (index) {
        return {
          scan: new IndexScan(index, tableObj.heap, columns, lowNode.value, highNode.value, alias || tableName),
          residual: null
        };
      }
    }
  }
  
  // Check AND: try to extract an indexed condition from one side
  if (where.type === 'AND') {
    const leftResult = tryIndexScan(where.left, fromClause, tables, indexCatalog);
    if (leftResult) {
      const combinedResidual = leftResult.residual
        ? { type: 'AND', left: leftResult.residual, right: where.right }
        : where.right;
      return { scan: leftResult.scan, residual: combinedResidual };
    }
    const rightResult = tryIndexScan(where.right, fromClause, tables, indexCatalog);
    if (rightResult) {
      const combinedResidual = rightResult.residual
        ? { type: 'AND', left: where.left, right: rightResult.residual }
        : where.left;
      return { scan: rightResult.scan, residual: combinedResidual };
    }
  }
  
  return null;
}

function extractColLiteral(compare, alias) {
  const left = compare.left;
  const right = compare.right;
  if (left?.type === 'column_ref' && (right?.type === 'literal' || right?.type === 'number')) {
    const name = left.name?.includes('.') ? left.name.split('.').pop() : left.name;
    return { colName: name, value: right.value, colSide: 'left' };
  }
  if (right?.type === 'column_ref' && (left?.type === 'literal' || left?.type === 'number')) {
    const name = right.name?.includes('.') ? right.name.split('.').pop() : right.name;
    return { colName: name, value: left.value, colSide: 'right' };
  }
  return {};
}

function findIndex(tableObj, colName, indexCatalog, tableName) {
  if (tableObj.indexes) {
    const idx = tableObj.indexes.get(colName);
    if (idx) return idx;
  }
  if (indexCatalog) {
    for (const [, idxInfo] of indexCatalog) {
      if (idxInfo.table === tableName && idxInfo.columns?.[0] === colName && idxInfo.index) {
        return idxInfo.index;
      }
    }
  }
  return null;
}

function buildScanNode(fromClause, tables) {
  if (!fromClause) return new ValuesIter([{}]); // Dummy for SELECT without FROM
  
  const tableName = typeof fromClause === 'string' ? fromClause : fromClause.table;
  const alias = typeof fromClause === 'string' ? null : fromClause.alias;
  
  const tableObj = tables.get(tableName) || tables.get(tableName.toLowerCase());
  if (!tableObj) throw new Error(`Table ${tableName} not found`);

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
    case 'binary_expr': {
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

/**
 * Split a WHERE clause into per-table and residual (cross-table) predicates.
 * Splits on AND conjuncts. Each conjunct is assigned to a table if it references
 * only that table's columns. Cross-table conditions become residual.
 */
function splitWhereByTable(where, tableNames) {
  const perTable = {};
  const residuals = [];
  
  // Split on AND
  const conjuncts = splitAnd(where);
  
  for (const conj of conjuncts) {
    const refs = collectTableRefs(conj);
    // Filter to only known table names
    const matchedTables = refs.filter(r => tableNames.includes(r));
    
    if (matchedTables.length === 1) {
      // Single-table predicate — push down
      const tableName = matchedTables[0];
      if (!perTable[tableName]) {
        perTable[tableName] = conj;
      } else {
        perTable[tableName] = { type: 'AND', left: perTable[tableName], right: conj };
      }
    } else if (matchedTables.length === 0 && refs.length === 0) {
      // Constant expression (e.g., 1 = 1) — can push anywhere or keep as residual
      residuals.push(conj);
    } else {
      // Cross-table or multi-table — residual
      residuals.push(conj);
    }
  }
  
  // Reconstruct residual
  let residual = null;
  for (const r of residuals) {
    residual = residual ? { type: 'AND', left: residual, right: r } : r;
  }
  
  return { perTable, residual };
}

function splitAnd(expr) {
  if (!expr) return [];
  if (expr.type === 'AND') {
    return [...splitAnd(expr.left), ...splitAnd(expr.right)];
  }
  return [expr];
}

function collectTableRefs(expr) {
  const refs = new Set();
  walkExpr(expr, node => {
    if (node.type === 'column_ref') {
      if (node.table) {
        refs.add(node.table);
      } else if (typeof node.name === 'string' && node.name.includes('.')) {
        refs.add(node.name.split('.')[0]);
      }
    }
  });
  return [...refs];
}

function walkExpr(expr, fn) {
  if (!expr || typeof expr !== 'object') return;
  fn(expr);
  if (expr.left) walkExpr(expr.left, fn);
  if (expr.right) walkExpr(expr.right, fn);
  if (expr.args) for (const arg of expr.args) walkExpr(arg, fn);
  if (expr.operand) walkExpr(expr.operand, fn);
  if (expr.value && typeof expr.value === 'object') walkExpr(expr.value, fn);
}
