// volcano-planner.js — Converts SQL AST to volcano iterator tree
// Bridges the SQL parser to the volcano execution engine

// --- Aggregate arg normalization helpers ---
// Parser emits arg as string for simple columns, AST node for expressions.
// These helpers eliminate the typeof checks scattered through the planner.
function aggArgStr(arg) {
  if (arg === '*') return '*';
  if (typeof arg === 'object') return arg.name || JSON.stringify(arg);
  return arg; // string
}
function isExprAgg(arg) {
  return typeof arg === 'object' && arg !== null && arg.type !== 'column_ref';
}
// Window function arg: extract column name (null for literals like NTILE(4))
function winArgName(arg) {
  if (arg == null || arg === '*') return arg;
  if (typeof arg === 'object') return arg.type === 'literal' ? null : (arg.name || null);
  return arg; // string
}
function winArgGetter(arg) {
  if (typeof arg === 'object' && arg !== null && arg.type !== 'literal' && arg.type !== 'column_ref') return buildValueGetter(arg);
  return null;
}

import {
  SeqScan, ValuesIter, Filter, Project, Limit, Distinct,
  NestedLoopJoin, HashJoin, Sort, HashAggregate, IndexNestedLoopJoin,
  IndexScan, Union, CTE as CTEIterator, Window,
} from './volcano.js';
import { likeToRegex } from './sql-functions.js';

/**
 * Build a plan and return the EXPLAIN output string.
 */
export function explainPlan(ast, tables, indexCatalog, tableStats) {
  const plan = buildPlan(ast, tables, indexCatalog, tableStats);
  return plan.explain();
}

/**
 * Build a volcano iterator tree from a SELECT AST and database tables.
 * @param {object} ast — parsed SELECT AST
 * @param {Map} tables — database tables map (name → { heap, schema, indexes })
 * @param {Map} [indexCatalog] — optional index catalog for INL join selection
 */
export function buildPlan(ast, tables, indexCatalog, tableStats) {
  // Create a local copy to avoid mutating the caller's tables map
  tables = new Map(tables);
  const _ctx = { tables, indexCatalog, tableStats };
  // Handle WITH (CTE) — parser may produce type='WITH' or type='SELECT' with ctes[]
  const hasCtes = (ast.type === 'WITH' && ast.ctes?.length > 0) || 
                  (ast.type === 'SELECT' && ast.ctes?.length > 0);
  if (hasCtes) {
    // Materialize each CTE as a temporary table
    const cteTables = new Map(tables);
    for (const cte of (ast.ctes || [])) {
      // Skip recursive CTEs — they're already materialized by _select as views
      if (cte.recursive) continue;
      
      const ctePlan = buildPlan(cte.query, cteTables, indexCatalog, tableStats);
      // Materialize: execute the CTE plan and store results
      ctePlan.open();
      const rows = [];
      let row;
      while ((row = ctePlan.next()) !== null) rows.push(row);
      ctePlan.close();
      
      // Handle UNION in CTE: cte.unionQuery is the right side
      if (cte.unionQuery) {
        try {
          const unionPlan = buildPlan(cte.unionQuery, cteTables, indexCatalog, tableStats);
          unionPlan.open();
          let uRow;
          while ((uRow = unionPlan.next()) !== null) rows.push(uRow);
          unionPlan.close();
        } catch (e) {
          // If union query fails, throw to fall back to legacy
          throw new Error('CTE UNION query failed: ' + e.message);
        }
        
        // For UNION (not ALL), deduplicate
        if (!cte.unionQuery.unionAll) {
          const seen = new Set();
          const uniqueRows = [];
          for (const r of rows) {
            const key = JSON.stringify(Object.entries(r).filter(([k]) => !k.startsWith('_')));
            if (!seen.has(key)) { seen.add(key); uniqueRows.push(r); }
          }
          rows.length = 0;
          rows.push(...uniqueRows);
        }
      }
      
      // Create a virtual table for the CTE
      // Strip table qualifications from column names (c.region → region)
      // CTE output columns should be unqualified so outer queries can re-qualify with their own alias
      const rawKeys = rows.length > 0 ? Object.keys(rows[0]).filter(k => !k.startsWith('_')) : [];
      const schema = rawKeys.map(name => {
        const unqual = name.includes('.') ? name.split('.').pop() : name;
        return { name: unqual, _origKey: name };
      });
      cteTables.set(cte.name, {
        heap: { 
          scan: function*() { for (const r of rows) yield { values: schema.map(c => r[c._origKey]), pageId: 0, slotIdx: 0 }; },
          rowCount: rows.length,
          tupleCount: rows.length
        },
        schema: schema.map(c => ({ name: c.name }))
      });
    }
    // Build the main query plan with CTE tables available
    const mainQuery = ast.type === 'WITH' ? ast.query : { ...ast, ctes: undefined };
    return buildPlan(mainQuery, cteTables, indexCatalog, tableStats);
  }

  // Handle UNION/UNION ALL
  if (ast.type === 'UNION') {
    const leftPlan = buildPlan(ast.left, tables, indexCatalog, tableStats);
    const rightPlan = buildPlan(ast.right, tables, indexCatalog, tableStats);
    let iter = new Union(leftPlan, rightPlan);
    if (!ast.all) {
      iter = new Distinct(iter);
    }
    return iter;
  }

  // 1. Build scan for FROM table
  // Handle SELECT without FROM (e.g., SELECT 1+1, SELECT NOW())
  if (!ast.from) {
    // Create a single-row virtual scan
    const singleRow = { __virtual: true };
    const virtualIter = {
      _opened: false,
      _done: false,
      _estimatedRows: 1,
      open() { this._opened = true; this._done = false; },
      next() { if (this._done) return null; this._done = true; return singleRow; },
      close() { this._done = true; },
      describe() { return { type: 'VirtualSingleRow' }; }
    };
    // Build projections directly
    const projections = buildProjections(ast.columns, false, _ctx);
    let iter = new Project(virtualIter, projections);
    // Handle LIMIT
    if (ast.limit != null || ast.offset != null) {
      iter = new Limit(iter, ast.limit ?? Infinity, ast.offset ?? 0);
    }
    return iter;
  }
  
  // Handle derived tables (subqueries in FROM)
  if (ast.from && ast.from.subquery) {
    // Check if subquery has unsupported features (window functions, etc.)
    const subAst = ast.from.subquery;
    const hasUnsupportedInSub = subAst.columns?.some(c => 
      c.type === 'window' || (c.type === 'expression' && c.expr?.over)
    );
    if (hasUnsupportedInSub) {
      throw new Error('Derived table contains unsupported features (window functions)');
    }
    const subPlan = buildPlan(ast.from.subquery, tables, indexCatalog, tableStats);
    subPlan.open();
    const subRows = [];
    let subRow;
    while ((subRow = subPlan.next()) !== null) subRows.push(subRow);
    subPlan.close();
    
    // Strip table qualifications from column names (same as CTE materialization)
    const rawKeys = subRows.length > 0 ? Object.keys(subRows[0]).filter(k => !k.startsWith('_')) : [];
    const subSchema = rawKeys.map(name => {
      const unqual = name.includes('.') ? name.split('.').pop() : name;
      return { name: unqual, _origKey: name };
    });
    const alias = ast.from.alias || '__derived';
    const virtualTable = {
      heap: {
        scan: function*() { for (const r of subRows) yield { values: subSchema.map(c => r[c._origKey]), pageId: 0, slotIdx: 0 }; },
        rowCount: subRows.length,
        tupleCount: subRows.length
      },
      schema: subSchema.map(c => ({ name: c.name }))
    };
    // Create a new tables map with the derived table added
    tables = new Map(tables);
    tables.set(alias, virtualTable);
    // Rewrite ast.from to reference the virtual table
    ast = { ...ast, from: { table: alias, alias } };
  }
  
  let iter = buildScanNode(ast.from, tables);
  const fromTableName = typeof ast.from === 'string' ? ast.from : ast.from.table;
  const fromTable = tables.get(fromTableName) || tables.get(fromTableName?.toLowerCase());
  const fromRowCount = fromTable?.heap?.rowCount || fromTable?.heap?.tupleCount || 100;
  iter._estimatedRows = fromRowCount;

  // Predicate pushdown: split WHERE into per-table conditions
  let residualWhere = ast.where;
  if (ast.where && ast.joins && ast.joins.length > 0) {
    const fromTableName = ast.from.alias || ast.from.table;
    const joinTableNames = ast.joins.map(j => j.alias || (typeof j.table === 'string' ? j.table : j.table));
    const allTables = [fromTableName, ...joinTableNames];
    
    const { perTable, residual } = splitWhereByTable(ast.where, allTables);
    
    // Push FROM table predicates into a Filter on the FROM scan
    if (perTable[fromTableName]) {
      const pred = buildPredicate(perTable[fromTableName], _ctx);
      iter = new Filter(iter, pred);
      const sel = estimateSelectivity(perTable[fromTableName], fromTableName, tableStats);
      iter._estimatedRows = Math.max(1, Math.round(fromRowCount * sel));
    }
    
    residualWhere = residual;
    
    // Store per-table predicates for join table pushdown
    var _pushdownPredicates = perTable;
  }

  // 2. Build JOIN nodes
  if (ast.joins && ast.joins.length > 0) {
    for (const join of ast.joins) {
      let rightTableName = typeof join.table === 'string' ? join.table : join.table;
      const rightAlias = join.alias || rightTableName;
      let rightIter;
      let rightRowCount = 100;
      
      // Handle derived tables (subquery in FROM/JOIN)
      if (!rightTableName && join.subquery) {
        // Materialize the subquery as a virtual table
        const subPlan = buildPlan(join.subquery, tables, indexCatalog, tableStats);
        if (subPlan) {
          subPlan.open();
          const rows = [];
          let row;
          while ((row = subPlan.next()) !== null) rows.push(row);
          subPlan.close();
          
          // Create virtual table for the derived table (SeqScan expects {values: [...]} tuples)
          const schema = rows.length > 0 ? Object.keys(rows[0]).filter(k => !k.startsWith('_')).map(name => ({ name })) : [];
          const colNames = schema.map(s => s.name);
          const virtualTable = {
            schema,
            heap: { 
              rowCount: rows.length, 
              tupleCount: rows.length, 
              scan: function*() { 
                for (const r of rows) yield { values: colNames.map(c => r[c]), pageId: 0, slotIdx: 0 }; 
              } 
            },
            indexes: new Map(),
          };
          tables.set(rightAlias, virtualTable);
          rightTableName = rightAlias;
          rightIter = buildScanNode({ table: rightAlias, alias: rightAlias }, tables);
          rightRowCount = rows.length || 1;
          rightIter._estimatedRows = rightRowCount;
        } else {
          continue; // Can't build subquery plan
        }
      } else {
        rightIter = buildScanNode({ table: rightTableName, alias: rightAlias }, tables);
        const rightTable = tables.get(rightTableName) || tables.get(rightTableName?.toLowerCase());
        rightRowCount = rightTable?.heap?.rowCount || rightTable?.heap?.tupleCount || 100;
        rightIter._estimatedRows = rightRowCount;
      }
      
      // Push per-table predicate into right scan (only for INNER/CROSS joins)
      // For LEFT/RIGHT/FULL joins, predicates on the nullable side must apply AFTER the join
      const canPushdownRight = !join.joinType || join.joinType === 'INNER' || join.joinType === 'CROSS';
      let deferredRightPredicate = null;
      if (_pushdownPredicates && _pushdownPredicates[rightAlias]) {
        if (canPushdownRight) {
          rightIter = new Filter(rightIter, buildPredicate(_pushdownPredicates[rightAlias], _ctx));
          const sel = estimateSelectivity(_pushdownPredicates[rightAlias], rightTableName, tableStats);
          rightIter._estimatedRows = Math.max(1, Math.round(rightRowCount * sel));
        } else {
          // Defer predicate to apply after the join
          deferredRightPredicate = _pushdownPredicates[rightAlias];
        }
      }
      
      // Handle NATURAL JOIN / USING: find common column names and create equi-join condition
      let effectiveOn = join.on;
      if (!effectiveOn) {
        let commonCols = null;
        if (join.natural) {
          const leftTable = tables.get(fromTableName) || tables.get(fromTableName?.toLowerCase());
          const rightTable = tables.get(rightTableName) || tables.get(rightTableName?.toLowerCase());
          if (leftTable?.schema && rightTable?.schema) {
            const leftCols = new Set(leftTable.schema.map(s => s.name));
            commonCols = rightTable.schema.filter(s => leftCols.has(s.name)).map(s => s.name);
          }
        } else if (join.usingColumns && join.usingColumns.length > 0) {
          commonCols = join.usingColumns;
        }
        
        if (commonCols && commonCols.length > 0) {
          const leftAlias = ast.from.alias || fromTableName;
          effectiveOn = commonCols.reduce((acc, col) => {
            const cond = {
              type: 'COMPARE', op: 'EQ',
              left: { type: 'column_ref', name: `${leftAlias}.${col}` },
              right: { type: 'column_ref', name: `${rightAlias}.${col}` }
            };
            return acc ? { type: 'AND', left: acc, right: cond } : cond;
          }, null);
        }
      }
      
      const predicate = effectiveOn ? buildPredicate(effectiveOn, _ctx) : null;
      
      // Choose join strategy:
      // 1. IndexNestedLoopJoin if inner table has usable index on join key
      // 2. HashJoin for equi-joins
      // 3. NestedLoopJoin as fallback
      const equiJoin = extractEquiJoinKeys(effectiveOn, ast.from, join);
      
      const isInnerJoin = !join.joinType || join.joinType === 'INNER' || join.joinType === 'CROSS';
      if (equiJoin && isInnerJoin) {
        const inlJoin = tryIndexNestedLoopJoin(
          iter, equiJoin, ast.from, join, tables
        );
        if (inlJoin) {
          inlJoin._estimatedRows = Math.max(1, iter._estimatedRows || fromRowCount);
          iter = inlJoin;
          // Apply pushdown predicate for the inner table as a post-filter
          if (_pushdownPredicates && _pushdownPredicates[rightAlias]) {
            iter = new Filter(iter, buildPredicate(_pushdownPredicates[rightAlias], _ctx));
            const sel = estimateSelectivity(_pushdownPredicates[rightAlias], rightTableName, tableStats);
            iter._estimatedRows = Math.max(1, Math.round((inlJoin._estimatedRows || fromRowCount) * sel));
          }
          continue;
        }
      }
      
      if (equiJoin) {
        const jt = join.joinType === 'LEFT' ? 'left' : join.joinType === 'RIGHT' ? 'right' : join.joinType === 'FULL' ? 'full' : 'inner';
        iter = new HashJoin(rightIter, iter, equiJoin.buildKey, equiJoin.probeKey, jt);
        // Join estimate using ndistinct: rows = max(left,right) / max(ndv_left,ndv_right)
        const leftEst = iter._probe?._estimatedRows || fromRowCount;
        const rightEst = rightIter._estimatedRows || rightRowCount;
        // Look up ndistinct for join columns
        const leftNdv = getColumnNdv(fromTableName, equiJoin.probeKey, tableStats);
        const rightNdv = getColumnNdv(rightTableName, equiJoin.buildKey, tableStats);
        const maxNdv = Math.max(leftNdv || 0, rightNdv || 0);
        if (maxNdv > 1) {
          iter._estimatedRows = Math.max(1, Math.round(Math.max(leftEst, rightEst) * Math.min(leftEst, rightEst) / maxNdv));
        } else {
          iter._estimatedRows = Math.max(1, Math.max(leftEst, rightEst));
        }
      } else {
        const jt = join.joinType === 'LEFT' ? 'left' : join.joinType === 'RIGHT' ? 'right' : join.joinType === 'FULL' ? 'full' : 'inner';
        iter = new NestedLoopJoin(iter, rightIter, predicate ? (l, r) => predicate({ ...l, ...r }) : null, jt);
        const leftEst = iter._outer?._estimatedRows || fromRowCount;
        const rightEst = rightIter._estimatedRows || rightRowCount;
        iter._estimatedRows = Math.max(1, Math.max(leftEst, rightEst));
      }
      
      // Apply deferred right-table predicate after outer join
      if (deferredRightPredicate) {
        iter = new Filter(iter, buildPredicate(deferredRightPredicate, _ctx));
      }
    }
  }

  // 3. WHERE filter — apply remaining (cross-table) predicates
  const effectiveWhere = residualWhere !== undefined ? residualWhere : ast.where;
  if (effectiveWhere) {
    const indexScanResult = tryIndexScan(effectiveWhere, ast.from, tables, indexCatalog, tableStats);
    if (indexScanResult) {
      // Replace SeqScan with IndexScan and apply remaining predicates
      iter = indexScanResult.scan;
      iter._estimatedRows = Math.max(1, Math.round((iter._estimatedRows || fromRowCount) * 0.1)); // Index = selective
      if (indexScanResult.residual) {
        iter = new Filter(iter, buildPredicate(indexScanResult.residual, _ctx));
        iter._estimatedRows = Math.max(1, Math.round((iter._estimatedRows || fromRowCount) * 0.5));
      }
    } else {
      const pred = buildPredicate(effectiveWhere, _ctx);
      iter = new Filter(iter, pred);
      const sel = estimateSelectivity(effectiveWhere, fromTableName, tableStats);
      iter._estimatedRows = Math.max(1, Math.round((iter._estimatedRows || fromRowCount) * sel));
    }
  }

  // 4. GROUP BY + aggregates
    // Recursively check if a node contains an aggregate
  function containsAggregate(node) {
    if (!node || typeof node !== 'object') return false;
    if (node.type === 'aggregate' || node.type === 'aggregate_expr') return true;
    // Don't recurse into scalar subqueries — their aggregates are independent
    if (node.type === 'scalar_subquery' || node.type === 'SUBQUERY') return false;
    return Object.values(node).some(v => {
      if (Array.isArray(v)) return v.some(containsAggregate);
      if (typeof v === 'object' && v !== null) return containsAggregate(v);
      return false;
    });
  }
  
  const hasAggregates = ast.columns.some(c => c.type === 'aggregate' || containsAggregate(c));
  let groupByExprs = [];
  let funcWrappedAggs = [];
  if (ast.groupBy || hasAggregates) {
    let groupBy = ast.groupBy || [];
    const aggregates = [];
    
    // Resolve GROUP BY aliases and ordinals
    groupBy = groupBy.map(gb => {
      // Ordinal: GROUP BY 1 → first column
      if (typeof gb === 'object' && gb.type === 'literal' && typeof gb.value === 'number') {
        const idx = gb.value - 1;
        if (idx >= 0 && idx < ast.columns.length) {
          const col = ast.columns[idx];
          // Expression column: return the expression itself
          if (col.type === 'expression' && col.expr) return col.expr;
          return col.name || col.alias || col;
        }
      }
      // String alias: GROUP BY d → resolve to actual column name or expression
      if (typeof gb === 'string') {
        const aliasMatch = ast.columns.find(c => c.alias === gb);
        if (aliasMatch) {
          // If it's an expression/function column, return the whole column object as expression
          if (aliasMatch.type === 'expression' && aliasMatch.expr) return aliasMatch.expr;
          if (aliasMatch.type === 'function' || aliasMatch.type === 'function_call') return aliasMatch;
          return aliasMatch.name || gb;
        }
      }
      return gb;
    });
    
    // Handle expression-based GROUP BY: pre-compute expressions as derived columns
    const resolvedGroupBy = [];
    for (let i = 0; i < groupBy.length; i++) {
      const gb = groupBy[i];
      if (typeof gb === 'object' && gb.type && gb.type !== 'column_ref') {
        // Expression GROUP BY — synthesize a column name and add a projection
        const syntheticName = `__group_expr_${i}`;
        groupByExprs.push({ name: syntheticName, getter: buildValueGetter(gb) });
        resolvedGroupBy.push(syntheticName);
      } else if (typeof gb === 'object' && gb.type === 'column_ref') {
        resolvedGroupBy.push(gb.name);
      } else {
        resolvedGroupBy.push(gb);
      }
    }
    
    // If there are expression GROUP BY, wrap with a projection that adds computed columns
    if (groupByExprs.length > 0) {
      const prevIter = iter;
      const prevEst = iter._estimatedRows;
      iter = {
        _child: prevIter,
        _estimatedRows: prevEst,
        open() { this._child.open(); },
        next() {
          const row = this._child.next();
          if (row === null) return null;
          // Add computed group-by columns
          for (const expr of groupByExprs) {
            row[expr.name] = expr.getter(row);
          }
          return row;
        },
        close() { this._child.close(); },
        describe() { return { type: 'ComputeGroupBy', children: [this._child] }; },
      };
    }
    
    funcWrappedAggs.length = 0; // Track function-wrapped aggregates for projection
    for (const col of ast.columns) {
      if (col.type === 'aggregate') {
        const argStr = aggArgStr(col.arg);
        const name = col.alias || `${col.func}(${argStr})`;
        const valueGetter = isExprAgg(col.arg) ? buildValueGetter(col.arg) : null;
        const filterPred = col.filter ? buildPredicate(col.filter) : null;
        aggregates.push({ name, func: col.func, column: argStr, valueGetter, filterPred, distinct: col.distinct, separator: col.separator });
      } else if ((col.type === 'function' || col.type === 'function_call') && 
                  col.args?.some(a => a.type === 'aggregate' || a.type === 'aggregate_expr')) {
        // Function-wrapped aggregate: COALESCE(SUM(val), 0), NULLIF(AVG(val), 0), etc.
        // Extract inner aggregates and track the wrapping function
        const innerAggs = [];
        for (const arg of col.args) {
          if (arg.type === 'aggregate' || arg.type === 'aggregate_expr') {
            const innerArgStr = aggArgStr(arg.arg);
            const syntheticName = `__func_agg_${aggregates.length}`;
            const valueGetter = isExprAgg(arg.arg) ? buildValueGetter(arg.arg) : null;
            aggregates.push({ name: syntheticName, func: arg.func, column: innerArgStr, valueGetter, distinct: arg.distinct });
            innerAggs.push({ syntheticName, originalArg: arg });
          }
        }
        funcWrappedAggs.push({ col, innerAggs });
      } else if (col.type === 'expression' && containsAggregate(col)) {
        // Expression containing aggregates: 100 * SUM(x) / SUM(y)
        // Extract all nested aggregates and track the expression
        const innerAggs = [];
        function extractAggs(node) {
          if (!node || typeof node !== 'object') return node;
          if (node.type === 'aggregate' || node.type === 'aggregate_expr') {
            const argStr = aggArgStr(node.arg);
            const syntheticName = `__expr_agg_${aggregates.length}`;
            const valueGetter = isExprAgg(node.arg) ? buildValueGetter(node.arg) : null;
            aggregates.push({ name: syntheticName, func: node.func, column: argStr, valueGetter, distinct: node.distinct, filterPred: node.filter ? buildPredicate(node.filter) : null });
            innerAggs.push({ syntheticName, originalNode: node });
            // Return a placeholder that buildValueGetter can resolve later
            return { type: 'column_ref', name: syntheticName };
          }
          // Recurse into object properties
          const result = Array.isArray(node) ? [...node] : { ...node };
          for (const key of Object.keys(result)) {
            if (key === 'type') continue;
            if (Array.isArray(result[key])) {
              result[key] = result[key].map(extractAggs);
            } else if (typeof result[key] === 'object' && result[key] !== null) {
              result[key] = extractAggs(result[key]);
            }
          }
          return result;
        }
        const rewrittenExpr = extractAggs(col.expr);
        funcWrappedAggs.push({ col, innerAggs, rewrittenExpr });
      }
    }
    
    // Collect aggregates from HAVING that aren't already in SELECT columns
    if (ast.having) {
      const havingAggs = [];
      (function collectHavingAggs(node) {
        if (!node || typeof node !== 'object') return;
        if (node.type === 'aggregate_expr') {
          havingAggs.push(node);
          return;
        }
        for (const key of Object.keys(node)) {
          if (key === 'type') continue;
          const val = node[key];
          if (Array.isArray(val)) val.forEach(collectHavingAggs);
          else if (typeof val === 'object' && val !== null) collectHavingAggs(val);
        }
      })(ast.having);
      for (const agg of havingAggs) {
        const argStr = aggArgStr(agg.arg);
        const name = `${agg.func}(${argStr})`;
        // Only add if not already present
        if (!aggregates.some(a => a.name === name)) {
          const valueGetter = isExprAgg(agg.arg) ? buildValueGetter(agg.arg) : null;
          aggregates.push({ name, func: agg.func, column: argStr, valueGetter, distinct: agg.distinct || false });
        }
      }
    }
    
    iter = new HashAggregate(iter, resolvedGroupBy, aggregates);
    // Estimate groups: sqrt(input rows) as rough heuristic
    const inputEst = iter._input?._estimatedRows || fromRowCount;
    iter._estimatedRows = Math.max(1, Math.round(Math.sqrt(inputEst)));

    // 5. HAVING filter (applied after aggregation)
    if (ast.having) {
      const havingPred = buildAggregatePredicate(ast.having, ast.columns, _ctx);
      iter = new Filter(iter, havingPred);
      iter._estimatedRows = Math.max(1, Math.round((iter._estimatedRows || 10) * 0.5));
    }
  }

  // 6. SELECT projection
  const projections = buildProjections(ast.columns, hasAggregates, _ctx);
  const hasWindowFns = ast.columns.some(c => c.type === 'window' || (c.type === 'expression' && containsWindow(c)));
  
  // Check if ORDER BY references columns not in SELECT
  const selectColNames = new Set(ast.columns.map(c => c.alias || c.name).filter(Boolean));
  const hasStar = ast.columns.some(c => c.type === 'star' || c.name === '*');
  const hiddenOrderCols = [];
  if (ast.orderBy && !hasStar) {
    for (const o of ast.orderBy) {
      const col = typeof o.column === 'object' ? (o.column.name || o.column) : o.column;
      if (typeof col === 'string' && !selectColNames.has(col)) {
        hiddenOrderCols.push(col);
      }
    }
  }
  const orderByUsesHidden = hiddenOrderCols.length > 0;
  
  if (projections && !hasAggregates && !hasWindowFns && !orderByUsesHidden) {
    // Don't project after aggregate or before window — column names already set
    const prevEst = iter._estimatedRows;
    iter = new Project(iter, projections);
    iter._estimatedRows = prevEst;
  } else if (projections && !hasAggregates && !hasWindowFns && orderByUsesHidden) {
    // Include hidden ORDER BY columns in projection for sort, strip after sort
    const tempProjections = [...projections];
    for (const hc of hiddenOrderCols) {
      tempProjections.push({ name: hc, expr: (row) => {
        if (row[hc] !== undefined) return row[hc];
        for (const k of Object.keys(row)) { if (k.endsWith('.' + hc)) return row[k]; }
        return null;
      }});
    }
    const prevEst = iter._estimatedRows;
    iter = new Project(iter, tempProjections);
    iter._estimatedRows = prevEst;
  } else if (hasAggregates) {
    // For aggregates, project to rename/select the right columns
    const prevEst = iter._estimatedRows;
    iter = new Project(iter, buildAggregateProjections(ast.columns, groupByExprs, funcWrappedAggs));
    iter._estimatedRows = prevEst;
  }

  // 6b. Window Functions
  // Detect window functions both as top-level columns and nested in expressions
  function containsWindow(node) {
    if (!node || typeof node !== 'object') return false;
    if (node.type === 'window') return true;
    return Object.values(node).some(v => {
      if (Array.isArray(v)) return v.some(containsWindow);
      if (typeof v === 'object' && v !== null) return containsWindow(v);
      return false;
    });
  }
  
  const windowCols = ast.columns.filter(c => c.type === 'window');
  const exprWindowCols = ast.columns.filter(c => c.type === 'expression' && containsWindow(c));
  
  if (windowCols.length > 0 || exprWindowCols.length > 0) {
    // Extract partition by, order by, and window function specs
    // Group by their OVER clause (same partition + order = same Window operator)
    const windowFuncs = windowCols.map(wc => {
      const partitionBy = (wc.over?.partitionBy || []).map(p => {
        if (typeof p === 'string') return p;
        if (p.type === 'column_ref') return p.name;
        return String(p);
      });
      const orderBy = (wc.over?.orderBy || []).map(o => ({
        column: typeof o.column === 'object' ? o.column.name : o.column,
        desc: o.direction === 'DESC',
      }));
      return {
        func: wc.func,
        name: wc.alias || `${wc.func}()`,
        arg: winArgName(wc.arg),
        argGetter: winArgGetter(wc.arg),
        ntile: wc.func === 'NTILE' && wc.arg?.type === 'literal' ? wc.arg.value : null,
        frame: wc.over?.frame || null,
        offset: wc.offset,
        defaultValue: wc.defaultValue,
        partitionBy,
        orderBy,
      };
    });
    
    // Extract window functions from expressions (CASE WHEN ROW_NUMBER() OVER ... = 1 THEN ...)
    const rewrittenExprCols = new Map(); // Map col → rewritten expression
    for (const ec of exprWindowCols) {
      function extractWindows(node) {
        if (!node || typeof node !== 'object') return node;
        if (node.type === 'window') {
          const syntheticName = `__win_${windowFuncs.length}`;
          const partitionBy = (node.over?.partitionBy || []).map(p => {
            if (typeof p === 'string') return p;
            if (p.type === 'column_ref') return p.name;
            return String(p);
          });
          const orderBy = (node.over?.orderBy || []).map(o => ({
            column: typeof o.column === 'object' ? o.column.name : o.column,
            desc: o.direction === 'DESC',
          }));
          windowFuncs.push({
            func: node.func,
            name: syntheticName,
            arg: winArgName(node.arg),
            argGetter: winArgGetter(node.arg),
            ntile: node.func === 'NTILE' && node.arg?.type === 'literal' ? node.arg.value : null,
            frame: node.over?.frame || null,
            offset: node.offset,
            defaultValue: node.defaultValue,
            partitionBy,
            orderBy,
          });
          return { type: 'column_ref', name: syntheticName };
        }
        // Recurse
        const result = Array.isArray(node) ? [...node] : { ...node };
        for (const key of Object.keys(result)) {
          if (key === 'type') continue;
          if (Array.isArray(result[key])) {
            result[key] = result[key].map(extractWindows);
          } else if (typeof result[key] === 'object' && result[key] !== null) {
            result[key] = extractWindows(result[key]);
          }
        }
        return result;
      }
      rewrittenExprCols.set(ec, extractWindows(ec.expr));
    }
    
    // Group window functions by their OVER spec (partition + order)
    const overGroups = new Map();
    for (const wf of windowFuncs) {
      const key = JSON.stringify({ partitionBy: wf.partitionBy, orderBy: wf.orderBy });
      if (!overGroups.has(key)) overGroups.set(key, { partitionBy: wf.partitionBy, orderBy: wf.orderBy, funcs: [] });
      overGroups.get(key).funcs.push(wf);
    }
    
    // Chain Window operators — one per unique OVER spec
    for (const group of overGroups.values()) {
      const prevEst = iter._estimatedRows;
      iter = new Window(iter, group.partitionBy, group.orderBy, group.funcs);
      iter._estimatedRows = prevEst;
    }
    
    // Apply projection after window — now window columns are available
    // But skip if ORDER BY uses hidden columns (will project after sort instead)
    if (projections && !orderByUsesHidden) {
      // Add window function columns to projections (they're already computed by Window)
      const finalProjections = [];
      for (const col of ast.columns) {
        if (col.type === 'window') {
          const name = col.alias || `${col.func}()`;
          finalProjections.push({ name, expr: (row) => row[name] });
        } else if (rewrittenExprCols.has(col)) {
          // Expression with extracted windows: evaluate rewritten expression
          const name = col.alias || `expr`;
          const getter = buildValueGetter(rewrittenExprCols.get(col));
          finalProjections.push({ name, expr: getter });
        } else if (col.type === 'star' || col.name === '*') {
          // Star: mark for special Project handling
          finalProjections.push({ name: '*', expr: (row) => row, star: true });
        } else {
          const name = col.alias || col.name;
          finalProjections.push({ name, expr: (row) => row[col.name] !== undefined ? row[col.name] : (row[name] !== undefined ? row[name] : (row[col.alias] !== undefined ? row[col.alias] : null)) });
        }
      }
      const prevEst2 = iter._estimatedRows;
      iter = new Project(iter, finalProjections);
      iter._estimatedRows = prevEst2;
    } else if (projections && orderByUsesHidden) {
      // Include SELECT columns + hidden ORDER BY columns for sort
      const tempProjections = [];
      for (const col of ast.columns) {
        if (col.type === 'window') {
          const name = col.alias || `${col.func}()`;
          tempProjections.push({ name, expr: (row) => row[name] });
        } else if (rewrittenExprCols && rewrittenExprCols.has(col)) {
          const name = col.alias || `expr`;
          const getter = buildValueGetter(rewrittenExprCols.get(col));
          tempProjections.push({ name, expr: getter });
        } else {
          const name = col.alias || col.name;
          tempProjections.push({ name, expr: (row) => {
            if (row[col.name] !== undefined) return row[col.name];
            if (row[name] !== undefined) return row[name];
            for (const k of Object.keys(row)) { if (k.endsWith('.' + (col.name || name))) return row[k]; }
            return null;
          }});
        }
      }
      // Add hidden ORDER BY columns
      for (const hc of hiddenOrderCols) {
        tempProjections.push({ name: hc, expr: (row) => {
          if (row[hc] !== undefined) return row[hc];
          for (const k of Object.keys(row)) { if (k.endsWith('.' + hc)) return row[k]; }
          return null;
        }});
      }
      const prevEst2 = iter._estimatedRows;
      iter = new Project(iter, tempProjections);
      iter._estimatedRows = prevEst2;
    }
  }

  // 7. DISTINCT
  if (ast.distinct) {
    const prevEst = iter._estimatedRows;
    iter = new Distinct(iter);
    iter._estimatedRows = prevEst; // Conservative
  }

  // 8. ORDER BY
  if (ast.orderBy && ast.orderBy.length > 0) {
    const orderSpec = ast.orderBy.map(o => ({
      column: resolveOutputColumn(o.column, ast.columns),
      desc: o.direction === 'DESC',
    }));
    const prevEst = iter._estimatedRows;
    iter = new Sort(iter, orderSpec);
    iter._estimatedRows = prevEst;
  }

  // 8b. Post-sort projection (strip hidden ORDER BY columns)
  if (orderByUsesHidden) {
    // Build final projection with only SELECT columns
    const finalProjections = [];
    for (const col of ast.columns) {
      if (col.type === 'window') {
        const name = col.alias || `${col.func}()`;
        finalProjections.push({ name, expr: (row) => row[name] });
      } else {
        const name = col.alias || col.name;
        finalProjections.push({ name, expr: (row) => row[col.name] !== undefined ? row[col.name] : (row[name] !== undefined ? row[name] : (row[col.alias] !== undefined ? row[col.alias] : null)) });
      }
    }
    const prevEst = iter._estimatedRows;
    iter = new Project(iter, finalProjections);
    iter._estimatedRows = prevEst;
  }

  // 9. LIMIT / OFFSET
  if (ast.limit != null || ast.offset != null) {
    const prevEst = iter._estimatedRows;
    const limit = ast.limit != null ? ast.limit : Infinity;
    const offset = ast.offset || 0;
    iter = new Limit(iter, limit, offset);
    if (ast.limit != null) {
      iter._estimatedRows = Math.min(prevEst || ast.limit, ast.limit);
    } else {
      iter._estimatedRows = Math.max(1, (prevEst || 100) - offset);
    }
  }

  return iter;
}

// --- Helpers ---

/**
 * Try to use an IndexScan for a WHERE predicate.
 * Looks for equality (col = val) or range (col BETWEEN low AND high) on indexed columns.
 * Returns { scan: IndexScan, residual: remainingWhere } or null.
 */
function tryIndexScan(where, fromClause, tables, indexCatalog, tableStats) {
  if (!where || !fromClause) return null;
  
  const tableName = typeof fromClause === 'string' ? fromClause : fromClause.table;
  const alias = typeof fromClause === 'string' ? null : fromClause.alias;
  const tableObj = tables.get(tableName) || tables.get(tableName?.toLowerCase());
  if (!tableObj) return null;
  
  const columns = tableObj.schema.map(c => c.name);
  const totalRows = tableObj.heap?.rowCount || tableObj.heap?.tupleCount || 0;
  
  // I/O cost model constants (PG-inspired)
  const SEQ_PAGE_COST = 1.0;
  const RANDOM_PAGE_COST = 4.0;
  const CPU_TUPLE_COST = 0.01;
  const CPU_INDEX_TUPLE_COST = 0.005;
  const ROWS_PER_PAGE = 100; // Approximate
  
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
        // Estimate selectivity using stats if available
        const sel = estimateSelectivity(where, tableName, tableStats);
        const estimatedRows = Math.max(1, Math.round(totalRows * sel));
        
        // Cost comparison: SeqScan vs IndexScan
        const numPages = Math.max(1, Math.ceil(totalRows / ROWS_PER_PAGE));
        const seqScanCost = numPages * SEQ_PAGE_COST + totalRows * CPU_TUPLE_COST;
        const indexPages = Math.max(1, Math.ceil(estimatedRows / ROWS_PER_PAGE));
        const indexScanCost = indexPages * RANDOM_PAGE_COST + estimatedRows * CPU_INDEX_TUPLE_COST + estimatedRows * CPU_TUPLE_COST;
        
        if (indexScanCost >= seqScanCost) {
          return null; // SeqScan is cheaper
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
    const leftResult = tryIndexScan(where.left, fromClause, tables, indexCatalog, tableStats);
    if (leftResult) {
      const combinedResidual = leftResult.residual
        ? { type: 'AND', left: leftResult.residual, right: where.right }
        : where.right;
      return { scan: leftResult.scan, residual: combinedResidual };
    }
    const rightResult = tryIndexScan(where.right, fromClause, tables, indexCatalog, tableStats);
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

function buildPredicate(expr, ctx) {
  if (!expr) return () => true;

  switch (expr.type) {
    case 'COMPARE': {
      const getLeft = buildValueGetter(expr.left, ctx);
      const getRight = buildValueGetter(expr.right, ctx);
      const cmp = comparators[expr.op];
      if (!cmp) throw new Error(`Unknown comparison: ${expr.op}`);
      return (row) => cmp(getLeft(row), getRight(row));
    }
    case 'AND': {
      const left = buildPredicate(expr.left, ctx);
      const right = buildPredicate(expr.right, ctx);
      return (row) => left(row) && right(row);
    }
    case 'OR': {
      const left = buildPredicate(expr.left, ctx);
      const right = buildPredicate(expr.right, ctx);
      return (row) => left(row) || right(row);
    }
    case 'QUANTIFIED_COMPARE': {
      // ANY/ALL: compare left value against all values from subquery
      const leftGetter = buildValueGetter(expr.left);
      const cmp = comparators[expr.op];
      const quantifier = expr.quantifier; // 'ANY' or 'ALL'
      
      // Eagerly evaluate subquery (not correlated)
      return (outerRow) => {
        const leftVal = leftGetter(outerRow);
        const subPlan = buildPlan(expr.subquery, ctx?.tables, ctx?.indexCatalog, ctx?.tableStats);
        if (!subPlan) return false;
        subPlan.open();
        const values = [];
        let r;
        while ((r = subPlan.next()) !== null) {
          values.push(Object.values(r)[0]); // scalar subquery
        }
        subPlan.close();
        
        if (quantifier === 'ANY' || quantifier === 'SOME') {
          return values.some(v => cmp(leftVal, v));
        } else { // ALL
          return values.every(v => cmp(leftVal, v));
        }
      };
    }
    case 'EXISTS': {
      // Correlated EXISTS: for each outer row, execute the subquery
      // replacing outer column references with outer row values
      const subAst = expr.subquery;
      return (outerRow) => {
        // Build a plan for the subquery, but we need to resolve outer references
        // Strategy: build predicate for subquery WHERE, substituting outer row values
        const subPlan = buildCorrelatedSubqueryPlan(subAst, outerRow, ctx);
        if (!subPlan) return false;
        subPlan.open();
        const hasRow = subPlan.next() !== null;
        subPlan.close();
        return hasRow;
      };
    }
    case 'NOT': {
      const inner = buildPredicate(expr.operand || expr.expr, ctx);
      return (row) => !inner(row);
    }
    case 'IS_NULL': {
      const getter = buildValueGetter(expr.left || expr.column || expr.expr);
      return (row) => getter(row) == null;
    }
    case 'IS_NOT_NULL': {
      const getter = buildValueGetter(expr.left || expr.column || expr.expr);
      return (row) => getter(row) != null;
    }
    case 'BETWEEN': {
      const val = buildValueGetter(expr.left || expr.expr);
      const low = buildValueGetter(expr.low);
      const high = buildValueGetter(expr.high);
      return (row) => { const v = val(row); return v >= low(row) && v <= high(row); };
    }
    case 'IN': {
      const val = buildValueGetter(expr.expr);
      const vals = expr.values.map(buildValueGetter);
      return (row) => { const v = val(row); return vals.some(g => g(row) === v); };
    }
    case 'IN_LIST': {
      // Parser variant: uses 'left' instead of 'expr'
      const val = buildValueGetter(expr.left || expr.expr);
      const vals = expr.values.map(buildValueGetter);
      return (row) => { const v = val(row); return vals.some(g => g(row) === v); };
    }
    case 'LIKE':
    case 'ILIKE': {
      const val = buildValueGetter(expr.left || expr.expr);
      const patternStr = typeof expr.pattern === 'object' ? expr.pattern.value : expr.pattern;
      const regex = new RegExp('^' + String(patternStr).replace(/%/g, '.*').replace(/_/g, '.') + '$', 'i');
      return (row) => regex.test(val(row));
    }
    case 'IN_HASHSET': {
      // Pre-computed hash set from optimizeSelect/decorrelate
      const val = buildValueGetter(expr.left);
      const hashSet = expr.hashSet;
      if (expr.negated) {
        return (row) => !hashSet.has(val(row));
      }
      return (row) => hashSet.has(val(row));
    }
    case 'CORRELATED_IN_HASHMAP': {
      // Batch-decorrelated correlated IN subquery: lookup per outer row via hash map
      const val = buildValueGetter(expr.left);
      const outerColGetters = expr.outerCols.map(col => buildValueGetter({ type: 'column_ref', name: col }));
      const hashMap = expr.hashMap;
      const negated = expr.negated || false;
      return (row) => {
        const keyParts = outerColGetters.map(g => g(row));
        const key = keyParts.length === 1 ? String(keyParts[0]) : keyParts.map(String).join('\0');
        const valueSet = hashMap.get(key);
        const inSet = valueSet ? valueSet.has(val(row)) : false;
        return negated ? !inSet : inSet;
      };
    }
    case 'IN_SUBQUERY': {
      const val = buildValueGetter(expr.left);
      if (ctx && ctx.tables) {
        const subAst = expr.subquery;
        
        // Detect if subquery is correlated (references outer table columns)
        const subTables = new Set();
        if (subAst.from) {
          const t = subAst.from.alias || subAst.from.table;
          if (t) subTables.add(t.toLowerCase());
        }
        if (subAst.joins) {
          for (const j of subAst.joins) {
            const t = j.alias || j.table;
            if (t) subTables.add(t.toLowerCase());
          }
        }
        const whereStr = JSON.stringify(subAst.where || {});
        const hasOuterRef = /column_ref.*?"name"\s*:\s*"([^"]+)"/.test(whereStr) &&
          [...whereStr.matchAll(/"name"\s*:\s*"([^"]+)"/g)].some(m => {
            const col = m[1];
            if (col.includes('.')) {
              const prefix = col.split('.')[0].toLowerCase();
              return !subTables.has(prefix);
            }
            return false;
          });
        
        if (hasOuterRef) {
          // Correlated IN: evaluate subquery per outer row
          const negated = !!expr.negated;
          return (outerRow) => {
            try {
              const subPlan = buildCorrelatedSubqueryPlan(subAst, outerRow, ctx);
              if (!subPlan) return negated; // no plan → treat as empty set
              subPlan.open();
              const values = new Set();
              let r;
              while ((r = subPlan.next()) !== null) {
                const keys = Object.keys(r);
                if (keys.length > 0) values.add(r[keys[0]]);
              }
              subPlan.close();
              const inSet = values.has(val(outerRow));
              return negated ? !inSet : inSet;
            } catch (e) { return negated; }
          };
        }
        
        // Non-correlated: evaluate once eagerly
        try {
          const subPlan = buildPlan(subAst, ctx.tables, ctx.indexCatalog, ctx.tableStats);
          if (subPlan) {
            subPlan.open();
            const values = new Set();
            let row;
            while ((row = subPlan.next()) !== null) {
              const keys = Object.keys(row);
              if (keys.length > 0) values.add(row[keys[0]]);
            }
            subPlan.close();
            if (expr.negated) {
              return (row) => !values.has(val(row));
            }
            return (row) => values.has(val(row));
          }
        } catch (e) { /* fall through */ }
      }
      return () => true;
    }
    case 'case_expr': {
      const whens = expr.whens.map(w => ({
        condition: buildPredicate(w.condition, ctx),
        result: buildPredicate(w.result, ctx)
      }));
      const elsePred = expr.elseResult || expr.else ? buildPredicate(expr.elseResult || expr.else, ctx) : () => false;
      return (row) => {
        for (const w of whens) {
          if (w.condition(row)) return w.result(row);
        }
        return elsePred(row);
      };
    }
    default:
      return () => true;
  }
}

function buildValueGetter(expr, ctx) {
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
          case '/': {
            if (r === 0) return null;
            const result = l / r;
            // SQL: INT / INT = INT (truncated)
            if (Number.isInteger(l) && Number.isInteger(r)) return Math.trunc(result);
            return result;
          }
          case '%': return l % r;
          default: return null;
        }
      };
    }
    case 'function_call':
    case 'function': {
      const args = (expr.args || []).map(a => buildValueGetter(a, ctx));
      const funcName = (expr.name || expr.func || '').toUpperCase();
      return (row) => {
        const vals = args.map(g => g(row));
        switch (funcName) {
          case 'ABS': return Math.abs(vals[0]);
          case 'UPPER': return String(vals[0]).toUpperCase();
          case 'LOWER': return String(vals[0]).toLowerCase();
          case 'LENGTH': return vals[0] != null ? String(vals[0]).length : null;
          case 'COALESCE': return vals.find(v => v != null) ?? null;
          case 'IFNULL': case 'NVL': return vals[0] != null ? vals[0] : vals[1];
          case 'TYPEOF': {
            const v = vals[0];
            if (v === null || v === undefined) return 'null';
            if (typeof v === 'number') return Number.isInteger(v) ? 'integer' : 'real';
            if (typeof v === 'string') return 'text';
            if (typeof v === 'boolean') return 'integer';
            return 'blob';
          }
          case 'CONCAT_OP': case 'CONCAT': return vals.map(v => v ?? '').join('');
          case 'SUBSTR': case 'SUBSTRING': return String(vals[0]).substring((vals[1] || 1) - 1, vals[2] ? (vals[1] || 1) - 1 + vals[2] : undefined);
          case 'TRIM': return vals[0] != null ? String(vals[0]).trim() : null;
          case 'REPLACE': return vals[0] != null ? String(vals[0]).replaceAll(String(vals[1]), String(vals[2])) : null;
          case 'ROUND': return vals[1] != null ? parseFloat(Number(vals[0]).toFixed(vals[1])) : Math.round(vals[0]);
          case 'NULLIF': return vals[0] === vals[1] ? null : vals[0];
          case 'GREATEST': return Math.max(...vals.filter(v => v != null));
          case 'LEAST': return Math.min(...vals.filter(v => v != null));
          case 'LEFT': return vals[0] != null ? String(vals[0]).substring(0, vals[1]) : null;
          case 'RIGHT': return vals[0] != null ? String(vals[0]).slice(-vals[1]) : null;
          default: return null;
        }
      };
    }
    case 'case_expr': {
      const whens = expr.whens.map(w => ({
        condition: buildPredicate(w.condition),
        result: buildValueGetter(w.result)
      }));
      const elseVal = expr.elseResult || expr.else ? buildValueGetter(expr.elseResult || expr.else) : () => null;
      return (row) => {
        for (const w of whens) {
          if (w.condition(row)) return w.result(row);
        }
        return elseVal(row);
      };
    }
    case 'CAST':
    case 'cast': {
      const inner = buildValueGetter(expr.expr);
      const dataType = (expr.targetType || expr.dataType || '').toUpperCase();
      return (row) => {
        const v = inner(row);
        if (v == null) return null;
        switch (dataType) {
          case 'INT': case 'INTEGER': case 'BIGINT': case 'SMALLINT': return parseInt(v, 10);
          case 'FLOAT': case 'DOUBLE': case 'REAL': case 'NUMERIC': case 'DECIMAL': return parseFloat(v);
          case 'TEXT': case 'VARCHAR': case 'CHAR': return String(v);
          case 'BOOLEAN': case 'BOOL': return Boolean(v);
          default: return v;
        }
      };
    }
    case 'SUBQUERY': {
      // Scalar subquery in expressions (WHERE salary > (SELECT AVG(...)))
      // Check if correlated by looking for outer references
      const subAst = expr.subquery;
      if (!ctx) return () => null;
      
      // Detect if subquery is correlated (references columns not in subquery's own tables)
      const subTables = new Set();
      if (subAst.from) {
        const t = subAst.from.alias || subAst.from.table;
        if (t) subTables.add(t.toLowerCase());
      }
      if (subAst.joins) {
        for (const j of subAst.joins) {
          const t = j.alias || j.table;
          if (t) subTables.add(t.toLowerCase());
        }
      }
      const whereStr = JSON.stringify(subAst.where || {});
      const hasOuterRef = /column_ref.*?"name"\s*:\s*"([^"]+)"/.test(whereStr) &&
        [...whereStr.matchAll(/"name"\s*:\s*"([^"]+)"/g)].some(m => {
          const col = m[1];
          if (col.includes('.')) {
            const prefix = col.split('.')[0].toLowerCase();
            return !subTables.has(prefix);
          }
          return false;
        });
      
      if (hasOuterRef) {
        // Correlated: evaluate per outer row
        return (outerRow) => {
          try {
            const subPlan = buildCorrelatedSubqueryPlan(subAst, outerRow, ctx);
            if (!subPlan) return null;
            subPlan.open();
            const row = subPlan.next();
            subPlan.close();
            if (row) {
              for (const [k, v] of Object.entries(row)) {
                if (!k.startsWith('_')) return v;
              }
            }
            return null;
          } catch (e) { return null; }
        };
      } else {
        // Non-correlated: evaluate once eagerly
        try {
          const subPlan = buildPlan(subAst, ctx.tables, ctx.indexCatalog, ctx.tableStats);
          if (subPlan) {
            subPlan.open();
            const row = subPlan.next();
            subPlan.close();
            if (row) {
              for (const [k, v] of Object.entries(row)) {
                if (!k.startsWith('_')) return () => v;
              }
            }
          }
        } catch (e) { /* fall through */ }
        return () => null;
      }
    }
    default:
      return () => null;
  }
}

const comparators = {
  EQ: (a, b) => a != null && b != null ? a == b : a == null && b == null ? null : false,
  '=': (a, b) => a != null && b != null ? a == b : a == null && b == null ? null : false,
  NE: (a, b) => a == null || b == null ? (a == null && b == null ? false : true) : a != b,
  NEQ: (a, b) => a == null || b == null ? (a == null && b == null ? false : true) : a != b,
  '!=': (a, b) => a == null || b == null ? (a == null && b == null ? false : true) : a != b,
  '<>': (a, b) => a == null || b == null ? (a == null && b == null ? false : true) : a != b,
  LT: (a, b) => a == null || b == null ? false : a < b,
  '<': (a, b) => a == null || b == null ? false : a < b,
  LE: (a, b) => a == null || b == null ? false : a <= b,
  '<=': (a, b) => a == null || b == null ? false : a <= b,
  GT: (a, b) => a == null || b == null ? false : a > b,
  '>': (a, b) => a == null || b == null ? false : a > b,
  GE: (a, b) => a == null || b == null ? false : a >= b,
  '>=': (a, b) => a == null || b == null ? false : a >= b,
};

/**
 * Try to build an IndexNestedLoopJoin if the inner table has a usable index.
 * Returns the INL join iterator, or null if no usable index found.
 */
function tryIndexNestedLoopJoin(outerIter, equiJoin, leftFrom, rightJoin, tables) {
  // Determine which side is outer (already built = left) and inner (right = join table)
  const rightTableName = typeof rightJoin.table === 'string' ? rightJoin.table : rightJoin.table;
  const rightAlias = rightJoin.alias || rightTableName;
  
  const rightTable = tables.get(rightTableName) || (rightTableName && tables.get(rightTableName.toLowerCase()));
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

/**
 * Build a plan for a correlated subquery, substituting outer row values.
 * Replaces outer column references in the WHERE with literal values.
 */
function buildCorrelatedSubqueryPlan(subAst, outerRow, ctx) {
  // Deep-clone the AST to avoid mutating the original
  const cloned = JSON.parse(JSON.stringify(subAst));
  
  // Get inner table names to distinguish inner vs outer references
  const innerTables = new Set();
  if (cloned.from) innerTables.add(cloned.from.alias || cloned.from.table);
  if (cloned.joins) for (const j of cloned.joins) innerTables.add(j.alias || j.table);
  
  // Substitute outer column references in WHERE with literal values
  if (cloned.where) {
    cloned.where = substituteOuterRefs(cloned.where, outerRow, innerTables);
  }
  
  try {
    return buildPlan(cloned, ctx?.tables, ctx?.indexCatalog, ctx?.tableStats);
  } catch (e) {
    return null;
  }
}

/**
 * Recursively substitute outer column references with literal values from outerRow.
 */
function substituteOuterRefs(node, outerRow, innerTables) {
  if (!node || typeof node !== 'object') return node;
  
  // If it's a column reference, check if it's an outer reference
  if (node.type === 'column_ref' && node.name && node.name.includes('.')) {
    const [table] = node.name.split('.');
    if (!innerTables.has(table)) {
      // This is an outer reference — substitute with literal value
      const val = outerRow[node.name];
      return { type: 'literal', value: val };
    }
  }
  
  // Recurse into all object properties
  const result = Array.isArray(node) ? [] : {};
  for (const [key, val] of Object.entries(node)) {
    if (typeof val === 'object' && val !== null) {
      result[key] = substituteOuterRefs(val, outerRow, innerTables);
    } else {
      result[key] = val;
    }
  }
  return result;
}

function extractEquiJoinKeys(on, leftFrom, rightJoin) {
  if (!on || on.type !== 'COMPARE' || on.op !== 'EQ') return null;
  if (on.left.type !== 'column_ref' || on.right.type !== 'column_ref') return null;
  
  // Figure out which column belongs to which table
  const leftAlias = leftFrom.alias || leftFrom.table || leftFrom;
  const rightAlias = rightJoin.alias || rightJoin.table;
  
  const lName = on.left.name;
  const rName = on.right.name;
  
  // Check if names are qualified (e.g., "o.product_id")
  if (lName.includes('.') && rName.includes('.')) {
    const [lTable] = lName.split('.');
    const [rTable] = rName.split('.');
    // Match qualified prefix to left/right table aliases
    // buildKey must come from the build side (rightIter = rightJoin table)
    // probeKey must come from the probe side (iter = leftFrom table)
    if (lTable === rightAlias && rTable === leftAlias) {
      return { buildKey: lName, probeKey: rName };
    }
    if (rTable === rightAlias && lTable === leftAlias) {
      return { buildKey: rName, probeKey: lName };
    }
    // Fallback: can't determine — return null to use NestedLoopJoin
    return null;
  }
  
  // Unqualified — just use the names directly (ambiguous but best effort)
  return { buildKey: rName, probeKey: lName };
}

function buildProjections(columns, hasAggregates, ctx) {
  if (!columns || columns.length === 0) return null;
  if (columns.length === 1 && columns[0].type === 'star') return null; // SELECT *
  if (hasAggregates) return null; // Handled separately
  
  return columns.map((col, idx) => {
    const outputName = col.alias || col.name || `col${idx}`;
    switch (col.type) {
      case 'column':
        return { name: outputName, expr: buildValueGetter({ type: 'column_ref', name: col.name }) };
      case 'star':
        return null; // Can't project * — passthrough
      case 'expression':
        return { name: outputName, expr: buildValueGetter(col.expr, ctx) };
      case 'function':
      case 'function_call':
        return { name: outputName, expr: buildValueGetter(col, ctx) };
      case 'scalar_subquery': {
        // Scalar subquery: (SELECT SUM(x) FROM t WHERE ...)
        if (!ctx) return { name: outputName || 'subquery', expr: () => null };
        
        // Detect if correlated by checking for outer table references in WHERE
        const subAst = col.subquery;
        const innerNames = new Set();
        if (subAst.from) innerNames.add((subAst.from.alias || subAst.from.table || '').toLowerCase());
        if (subAst.joins) for (const j of subAst.joins) innerNames.add((j.alias || j.table || '').toLowerCase());
        const whereStr = JSON.stringify(subAst.where || {});
        const hasOuterRefs = [...whereStr.matchAll(/"name"\s*:\s*"([^"]+)"/g)].some(m => {
          const name = m[1];
          if (name.includes('.')) {
            const prefix = name.split('.')[0].toLowerCase();
            return !innerNames.has(prefix);
          }
          return false;
        });
        
        if (!hasOuterRefs) {
          // Non-correlated: pre-evaluate once
          try {
            const subPlan = buildPlan(subAst, ctx.tables, ctx.indexCatalog, ctx.tableStats);
            subPlan.open();
            const row = subPlan.next();
            subPlan.close();
            if (row) {
              for (const [k, v] of Object.entries(row)) {
                if (!k.startsWith('_')) return { name: outputName || 'subquery', expr: () => v };
              }
            }
            return { name: outputName || 'subquery', expr: () => null };
          } catch (e) { /* fall through to correlated */ }
        }
        
        // Correlated: evaluate per outer row
        return { name: outputName || 'subquery', expr: (outerRow) => {
          try {
            const subPlan = buildCorrelatedSubqueryPlan(subAst, outerRow, ctx);
            if (!subPlan) return null;
            subPlan.open();
            const row = subPlan.next();
            subPlan.close();
            if (row) {
              for (const [k, v] of Object.entries(row)) {
                if (!k.startsWith('_')) return v;
              }
            }
            return null;
          } catch (e) { return null; }
        }};
      }
      default:
        return { name: outputName, expr: () => null };
    }
  }).filter(Boolean);
}

function buildAggregateProjections(columns, groupByExprs, funcWrappedAggs) {
  return columns.map((col, idx) => {
    if (col.type === 'aggregate') {
      const argStr = aggArgStr(col.arg);
      const name = col.alias || `${col.func}(${argStr})`;
      return { name: col.alias || name, expr: (row) => row[name] };
    }
    // Function-wrapped aggregate: COALESCE(SUM(val), 0) etc.
    if ((col.type === 'function' || col.type === 'function_call') && funcWrappedAggs) {
      const fwa = funcWrappedAggs.find(f => f.col === col);
      if (fwa) {
        const outputName = col.alias || `${col.func}(...)`;
        const outerFunc = col.func?.toUpperCase();
        return { name: outputName, expr: (row) => {
          // Build args: replace aggregate args with their computed values, keep literals
          const args = col.args.map(arg => {
            if (arg.type === 'aggregate' || arg.type === 'aggregate_expr') {
              const match = fwa.innerAggs.find(ia => ia.originalArg === arg);
              return match ? row[match.syntheticName] : null;
            }
            if (arg.type === 'literal') return arg.value;
            return null;
          });
          // Apply outer function
          return applyScalarFunction(outerFunc, args);
        }};
      }
    }
    if (col.type === 'column') {
      const name = col.alias || col.name;
      return { name, expr: (row) => row[col.name] !== undefined ? row[col.name] : (row[name] !== undefined ? row[name] : null) };
    }
    // Expression columns (e.g., CASE WHEN ... AS band): look up in synthetic group columns
    if (col.type === 'expression' && funcWrappedAggs) {
      const fwa = funcWrappedAggs.find(f => f.col === col);
      if (fwa && fwa.rewrittenExpr) {
        // Expression containing aggregates: evaluate rewritten expression using aggregate results
        const outputName = col.alias || `expr${idx}`;
        const getter = buildValueGetter(fwa.rewrittenExpr);
        return { name: outputName, expr: getter };
      }
    }
    if (col.type === 'expression' && groupByExprs && groupByExprs.length > 0) {
      const name = col.alias || 'expr';
      // Try to find a matching synthetic group expression
      for (const ge of groupByExprs) {
        return { name, expr: (row) => row[ge.name] };
      }
    }
    return { name: col.alias || 'expr', expr: () => null };
  });
}

function buildAggregatePredicate(expr, columns, ctx) {
  // HAVING uses aggregate expressions — resolve to their output names
  if (expr.type === 'COMPARE') {
    const getLeft = buildAggregateValueGetter(expr.left, columns, ctx);
    const getRight = buildAggregateValueGetter(expr.right, columns, ctx);
    const cmp = comparators[expr.op];
    return (row) => cmp(getLeft(row), getRight(row));
  }
  if (expr.type === 'AND') {
    const left = buildAggregatePredicate(expr.left, columns, ctx);
    const right = buildAggregatePredicate(expr.right, columns, ctx);
    return (row) => left(row) && right(row);
  }
  if (expr.type === 'OR') {
    const left = buildAggregatePredicate(expr.left, columns, ctx);
    const right = buildAggregatePredicate(expr.right, columns, ctx);
    return (row) => left(row) || right(row);
  }
  if (expr.type === 'NOT') {
    const inner = buildAggregatePredicate(expr.expr, columns, ctx);
    return (row) => !inner(row);
  }
  if (expr.type === 'BETWEEN') {
    const getVal = buildAggregateValueGetter(expr.left, columns, ctx);
    const getLow = buildAggregateValueGetter(expr.low, columns, ctx);
    const getHigh = buildAggregateValueGetter(expr.high, columns, ctx);
    return (row) => {
      const v = getVal(row), lo = getLow(row), hi = getHigh(row);
      return v >= lo && v <= hi;
    };
  }
  if (expr.type === 'NOT_BETWEEN') {
    const getVal = buildAggregateValueGetter(expr.left, columns, ctx);
    const getLow = buildAggregateValueGetter(expr.low, columns, ctx);
    const getHigh = buildAggregateValueGetter(expr.high, columns, ctx);
    return (row) => {
      const v = getVal(row), lo = getLow(row), hi = getHigh(row);
      return v < lo || v > hi;
    };
  }
  if (expr.type === 'IN_LIST') {
    const getLeft = buildAggregateValueGetter(expr.left, columns, ctx);
    const getters = expr.values.map(v => buildAggregateValueGetter(v, columns, ctx));
    return (row) => {
      const val = getLeft(row);
      return getters.some(g => g(row) === val);
    };
  }
  if (expr.type === 'IS_NULL') {
    const getVal = buildAggregateValueGetter(expr.left, columns, ctx);
    return (row) => { const v = getVal(row); return v === null || v === undefined; };
  }
  if (expr.type === 'IS_NOT_NULL') {
    const getVal = buildAggregateValueGetter(expr.left, columns, ctx);
    return (row) => { const v = getVal(row); return v !== null && v !== undefined; };
  }
  if (expr.type === 'LIKE' || expr.type === 'ILIKE') {
    const getVal = buildAggregateValueGetter(expr.left, columns, ctx);
    const getPat = buildAggregateValueGetter(expr.pattern, columns, ctx);
    const caseInsensitive = expr.type === 'ILIKE';
    return (row) => {
      const val = getVal(row), pat = getPat(row);
      if (val == null || pat == null) return null;
      const regex = likeToRegex(String(pat));
      return new RegExp(regex, caseInsensitive ? 'i' : '').test(String(val));
    };
  }
  if (expr.type === 'NOT_LIKE') {
    const getVal = buildAggregateValueGetter(expr.left, columns, ctx);
    const getPat = buildAggregateValueGetter(expr.pattern, columns, ctx);
    return (row) => {
      const val = getVal(row), pat = getPat(row);
      if (val == null || pat == null) return null;
      const regex = likeToRegex(String(pat));
      return !new RegExp(regex).test(String(val));
    };
  }
  if (expr.type === 'EXISTS') {
    // EXISTS subquery in HAVING
    if (ctx) {
      try {
        const subPlan = buildPlan(expr.subquery, ctx.tables, ctx.indexCatalog, ctx.tableStats);
        if (subPlan) {
          subPlan.open();
          const hasRow = subPlan.next() !== null;
          subPlan.close();
          return () => hasRow;
        }
      } catch (e) { /* fall through */ }
    }
    return () => false;
  }
  return () => true;
}

function buildAggregateValueGetter(expr, columns, ctx) {
  if (expr.type === 'literal') return () => expr.value;
  if (expr.type === 'SUBQUERY') {
    // Evaluate subquery eagerly (once) and return the scalar result
    if (ctx) {
      try {
        const subPlan = buildPlan(expr.subquery, ctx.tables, ctx.indexCatalog, ctx.tableStats);
        if (subPlan) {
          subPlan.open();
          const row = subPlan.next();
          subPlan.close();
          if (row) {
            const val = Object.values(row)[0]; // scalar subquery = first column of first row
            return () => val;
          }
        }
      } catch (e) { /* fall through */ }
    }
    return () => null;
  }
  if (expr.type === 'aggregate_expr') {
    // Find the matching aggregate column's output name
    // Normalize arg: parser may give {type:'column_ref', name:'val'} or just 'val'
    const exprArg = aggArgStr(expr.arg);
    const match = columns.find(c => c.type === 'aggregate' && c.func === expr.func && 
      (aggArgStr(c.arg) === exprArg));
    const name = match ? (match.alias || `${match.func}(${aggArgStr(match.arg)})`) : `${expr.func}(${exprArg})`;
    return (row) => row[name];
  }
  if (expr.type === 'column_ref') {
    const name = expr.name;
    return (row) => row[name];
  }
  if (expr.type === 'function_call' || expr.type === 'function') {
    // Function wrapping aggregate args: COALESCE(SUM(x), 0), NULLIF(AVG(x), 0), etc.
    const argGetters = (expr.args || []).map(a => buildAggregateValueGetter(a, columns, ctx));
    const fn = expr.func.toUpperCase();
    return (row) => {
      const args = argGetters.map(g => g(row));
      switch (fn) {
        case 'COALESCE': return args.find(a => a !== null && a !== undefined) ?? null;
        case 'NULLIF': return args[0] === args[1] ? null : args[0];
        case 'GREATEST': return Math.max(...args.filter(a => a != null));
        case 'LEAST': return Math.min(...args.filter(a => a != null));
        case 'ABS': return args[0] != null ? Math.abs(args[0]) : null;
        case 'ROUND': return args[0] != null ? Math.round(args[0] * (10 ** (args[1] || 0))) / (10 ** (args[1] || 0)) : null;
        case 'CEIL': case 'CEILING': return args[0] != null ? Math.ceil(args[0]) : null;
        case 'FLOOR': return args[0] != null ? Math.floor(args[0]) : null;
        default: return null;
      }
    };
  }
  if (expr.type === 'arith' || expr.type === 'binary_expr') {
    // Arithmetic in HAVING: SUM(x) * 100
    const getLeft = buildAggregateValueGetter(expr.left, columns, ctx);
    const getRight = buildAggregateValueGetter(expr.right, columns, ctx);
    const op = expr.op;
    return (row) => {
      const l = getLeft(row), r = getRight(row);
      if (l == null || r == null) return null;
      switch (op) {
        case '+': return l + r;
        case '-': return l - r;
        case '*': return l * r;
        case '/': return r === 0 ? null : l / r;
        case '%': return r === 0 ? null : l % r;
        default: return null;
      }
    };
  }
  return buildValueGetter(expr);
}

function resolveOutputColumn(name, columns) {
  // Ordinal: ORDER BY 1 → first column
  if (typeof name === 'number' || (typeof name === 'object' && name?.type === 'literal' && typeof name?.value === 'number')) {
    const idx = (typeof name === 'number' ? name : name.value) - 1;
    if (idx >= 0 && idx < columns.length) {
      const col = columns[idx];
      if (col.alias) return col.alias;
      if (col.type === 'aggregate') {
        const argStr = aggArgStr(col.arg);
        return col.alias || `${col.func}(${argStr})`;
      }
      return col.name || col.alias;
    }
  }
  
  // Order by might reference an alias
  if (typeof name === 'string') {
    for (const col of columns) {
      if (col.alias === name) {
        if (col.type === 'aggregate') {
          const argStr = aggArgStr(col.arg);
          return col.alias || `${col.func}(${argStr})`;
        }
        return col.alias || col.name;
      }
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

/**
 * Estimate selectivity of a WHERE predicate using table statistics.
 * Returns fraction (0..1) of rows expected to pass the predicate.
 */
function estimateSelectivity(where, tableName, tableStats) {
  if (!where || !tableStats) return 0.33; // default heuristic
  
  const stats = tableStats.get(tableName);
  if (!stats || !stats.columns) return 0.33;
  
  if (where.type === 'COMPARE') {
    const colName = where.left?.name?.includes('.') 
      ? where.left.name.split('.').pop() 
      : where.left?.name;
    const colStats = colName ? stats.columns[colName] : null;
    
    if (where.op === 'EQ' && colStats?.distinct > 0) {
      // Equality: selectivity = 1/ndistinct
      return 1 / colStats.distinct;
    }
    if (['LT', 'LE', 'GT', 'GE'].includes(where.op)) {
      // Range: use histogram if available, else default 33%
      if (colStats?.histogram && colStats.histogram.length > 0) {
        const val = where.right?.value ?? where.left?.value;
        if (val != null) {
          const h = colStats.histogram;
          const totalCount = h.reduce((s, b) => s + b.count, 0);
          if (totalCount > 0) {
            let matchingCount = 0;
            for (const bucket of h) {
              if (where.op === 'GT' || where.op === 'GE') {
                // col > val: count rows in buckets where hi >= val
                if (bucket.lo >= val) matchingCount += bucket.count;
                else if (bucket.hi >= val) {
                  // Partial bucket: interpolate
                  const frac = (bucket.hi - val) / Math.max(1, bucket.hi - bucket.lo);
                  matchingCount += Math.round(bucket.count * frac);
                }
              } else { // LT or LE
                // col < val: count rows in buckets where lo <= val
                if (bucket.hi <= val) matchingCount += bucket.count;
                else if (bucket.lo <= val) {
                  const frac = (val - bucket.lo) / Math.max(1, bucket.hi - bucket.lo);
                  matchingCount += Math.round(bucket.count * frac);
                }
              }
            }
            return Math.max(0.01, matchingCount / totalCount);
          }
        }
      }
      return 0.33;
    }
  }
  
  if (where.type === 'AND') {
    const left = estimateSelectivity(where.left, tableName, tableStats);
    const right = estimateSelectivity(where.right, tableName, tableStats);
    return left * right; // Independence assumption
  }
  
  if (where.type === 'OR') {
    const left = estimateSelectivity(where.left, tableName, tableStats);
    const right = estimateSelectivity(where.right, tableName, tableStats);
    return Math.min(1, left + right - left * right);
  }
  
  return 0.33;
}

/**
 * Get ndistinct for a column from ANALYZE stats.
 */
function getColumnNdv(tableName, columnName, tableStats) {
  if (!tableStats || !tableName || !columnName) return null;
  // Strip table alias prefix from column name (e.g., "o.customer" → "customer")
  const col = columnName.includes('.') ? columnName.split('.').pop() : columnName;
  const stats = tableStats.get(tableName);
  if (!stats || !stats.columns) return null;
  return stats.columns[col]?.distinct || null;
}

/**
 * Apply a scalar SQL function to pre-computed argument values.
 */
function applyScalarFunction(funcName, args) {
  switch (funcName) {
    case 'COALESCE': return args.find(v => v != null) ?? null;
    case 'NULLIF': return args[0] === args[1] ? null : args[0];
    case 'GREATEST': return Math.max(...args.filter(v => v != null));
    case 'LEAST': return Math.min(...args.filter(v => v != null));
    case 'ABS': return Math.abs(args[0]);
    case 'ROUND': return args[1] != null ? parseFloat(Number(args[0]).toFixed(args[1])) : Math.round(args[0]);
    case 'UPPER': return args[0] != null ? String(args[0]).toUpperCase() : null;
    case 'LOWER': return args[0] != null ? String(args[0]).toLowerCase() : null;
    case 'LENGTH': return args[0] != null ? String(args[0]).length : null;
    case 'CONCAT': case 'CONCAT_OP': return args.map(v => v ?? '').join('');
    case 'TRIM': return args[0] != null ? String(args[0]).trim() : null;
    case 'REPLACE': return args[0] != null ? String(args[0]).replaceAll(String(args[1]), String(args[2])) : null;
    case 'LEFT': return args[0] != null ? String(args[0]).substring(0, args[1]) : null;
    case 'RIGHT': return args[0] != null ? String(args[0]).slice(-args[1]) : null;
    case 'IFNULL': return args[0] != null ? args[0] : args[1]; // alias of COALESCE for 2 args
    default: return null;
  }
}
