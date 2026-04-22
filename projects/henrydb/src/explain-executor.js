// explain-executor.js — EXPLAIN/EXPLAIN ANALYZE extracted from db.js
// Functions take 'db' as first parameter (database context)

import { PlanBuilder, PlanFormatter } from './query-plan.js';
import { planToHTML } from './plan-html.js';
import { QueryPlanner } from './planner.js';
import { explainPlan as volcanoExplainPlan, buildPlan as volcanoBuildPlan } from './volcano-planner.js';
import { instrumentPlan } from './volcano.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { BTreeTable } from './btree-table.js';

export function explain(db, ast) {
  const stmt = ast.statement;
  const format = ast.format || 'text';

  // EXPLAIN COMPILED: show the compiled query plan
  if (ast.compiled) {
    return explainCompiled(db, stmt);
  }

  // EXPLAIN ANALYZE: execute the query and measure actual performance
  if (ast.analyze) {
    return explainAnalyze(db, stmt);
  }

  // Tree-structured plan (new system) — use for SELECT statements
  if (stmt.type === 'SELECT' && (format === 'volcano' || format === 'tree' || format === 'json-tree' || format === 'html' || format === 'dot' || format === 'yaml')) {
    // Volcano format: show the Volcano iterator plan tree
    if (format === 'volcano') {
      try {
        const volcanoTree = volcanoExplainPlan(stmt, db.tables, db._indexes, db._tableStats);
        const lines = volcanoTree.split('\n');
        return { type: 'PLAN', rows: lines.map(l => ({ 'QUERY PLAN': l })) };
      } catch (e) {
        // Fall through to legacy explain if Volcano planner can't handle this query
        return { type: 'PLAN', rows: [{ 'QUERY PLAN': `Volcano planner error: ${e.message}` }] };
      }
    }
    const builder = new PlanBuilder(this);
    const planTree = builder.buildPlan(stmt);
    if (format === 'json-tree') {
      const json = PlanFormatter.toJSON(planTree);
      return { type: 'PLAN', rows: [{ 'QUERY PLAN': JSON.stringify([json], null, 2) }] };
    }
    if (format === 'html') {
      const html = planToHTML(planTree);
      return { type: 'PLAN', rows: [{ 'QUERY PLAN': html }], html };
    }
    if (format === 'dot') {
      const dot = PlanFormatter.toDOT(planTree);
      return { type: 'PLAN', rows: [{ 'QUERY PLAN': dot }], dot };
    }
    if (format === 'yaml') {
      const yaml = PlanFormatter.toYAML(planTree);
      return { type: 'PLAN', rows: [{ 'QUERY PLAN': yaml }], yaml };
    }
    const lines = PlanFormatter.format(planTree);
    return { type: 'PLAN', rows: lines.map(l => ({ 'QUERY PLAN': l })) };
  }

  const plan = [];

  if (stmt.type !== 'SELECT') {
    return { type: 'PLAN', plan: [{ operation: 'UNKNOWN', detail: stmt.type }] };
  }

  // CTE analysis
  if (stmt.ctes) {
    for (const cte of stmt.ctes) {
      plan.push({ operation: 'CTE', name: cte.name, recursive: cte.recursive || false });
    }
  }

  const tableName = stmt.from?.table;
  const hasJoins = stmt.joins && stmt.joins.length > 0;

  // Check view
  if (tableName && db.views.has(tableName)) {
    plan.push({ operation: 'VIEW_SCAN', view: tableName });
  } else if (tableName && db.tables.has(tableName)) {
    const table = db.tables.get(tableName);

    // Determine scan type
    const estRows = db._estimateRowCount(table);
    const engine = table.heap instanceof BTreeTable ? 'btree' : 'heap';
    const filterEst = stmt.where ? db._estimateFilteredRows(tableName, stmt.where, estRows) : null;
    if (!hasJoins && stmt.where) {
      const indexScan = db._tryIndexScan(table, stmt.where, stmt.from.alias || tableName);
      if (indexScan !== null) {
        const estimatedResultRows = filterEst?.estimated || (indexScan.btreeLookup ? 1 : indexScan.rows.length);
        const costComparison = db._compareScanCosts(estRows, estimatedResultRows);
        
        if (indexScan.btreeLookup) {
          // BTree PK lookup is always fast (O(log N)), always use it
          const pkCol = table.schema.find(c => c.primaryKey)?.name || 'id';
          plan.push({ operation: 'INDEX_SCAN', table: tableName, index: pkCol, engine, estimated_rows: estimatedResultRows, estimation_method: filterEst?.method, cost: costComparison.indexCost });
        } else if (costComparison.useIndex || costComparison.selectivity <= 0.5 || estRows <= 100) {
          // Index scan is cheaper, or table is small enough that index overhead is negligible
          // For small tables (<=100 rows), always prefer explicit secondary index for equality
          const colName = db._findIndexedColumn(stmt.where);
          plan.push({ operation: 'INDEX_SCAN', table: tableName, index: colName, engine, estimated_rows: estimatedResultRows, estimation_method: filterEst?.method, cost: costComparison.indexCost, cost_comparison: `idx=${costComparison.indexCost} seq=${costComparison.seqCost} sel=${costComparison.selectivity}` });
        } else {
          // Sequential scan is cheaper despite available index
          plan.push({ operation: 'TABLE_SCAN', table: tableName, engine, estimated_rows: estRows, filtered_estimate: filterEst?.estimated, estimation_method: filterEst?.method, cost: costComparison.seqCost, cost_comparison: `seq=${costComparison.seqCost} idx=${costComparison.indexCost} sel=${costComparison.selectivity}`, index_available: true, index_rejected: 'seq scan cheaper' });
          plan.push({ operation: 'FILTER', condition: 'WHERE' });
        }
      } else {
        plan.push({ operation: 'TABLE_SCAN', table: tableName, engine, estimated_rows: estRows, filtered_estimate: filterEst?.estimated, estimation_method: filterEst?.method });
        plan.push({ operation: 'FILTER', condition: 'WHERE' });
      }
    } else {
      plan.push({ operation: 'TABLE_SCAN', table: tableName, engine, estimated_rows: estRows });
    }

    // Joins — show optimized order if applicable
    let joinList = stmt.joins || [];
    const originalOrder = joinList.map(j => j.table?.table || j.table);
    if (joinList.length >= 2 && tableName) {
      joinList = db._optimizeJoinOrder(tableName, joinList);
    }
    const optimizedOrder = joinList.map(j => j.table?.table || j.table);
    const wasReordered = JSON.stringify(originalOrder) !== JSON.stringify(optimizedOrder);
    
    if (wasReordered) {
      plan.push({
        operation: 'JOIN_REORDER',
        original: originalOrder.join(' → '),
        optimized: optimizedOrder.join(' → '),
        reason: 'cost-based (DP enumeration)',
      });
    }
    
    for (const join of joinList) {
      const joinTable = join.table?.table || join.table;
      const equiJoinKey = join.on ? db._extractEquiJoinKey(join.on, stmt.from.alias || tableName, join.alias || joinTable) : null;
      const isSelfJoin = joinTable === tableName;
      const joinEntry = {
        operation: equiJoinKey ? 'HASH_JOIN' : 'NESTED_LOOP_JOIN',
        type: join.type || 'INNER',
        table: joinTable,
        on: equiJoinKey ? `${equiJoinKey.leftKey} = ${equiJoinKey.rightKey}` : 'complex condition',
        selfJoin: isSelfJoin || undefined,
      };
      
      // Add cost estimate if stats available
      if (equiJoinKey) {
        const rightTbl = db.tables.get(joinTable);
        if (rightTbl) {
          const rightRows = db._estimateRowCount(rightTbl);
          joinEntry.estimated_right_rows = rightRows;
        }
      }
      
      plan.push(joinEntry);
    }
  }

  // WHERE (if not already noted)
  if (stmt.where && !plan.some(p => p.operation === 'FILTER')) {
    plan.push({ operation: 'FILTER', condition: 'WHERE' });
  }

  // GROUP BY
  if (stmt.groupBy) {
    // Estimate group count from ANALYZE ndistinct
    let groupEstimate = null;
    const stats = db._tableStats?.get(tableName);
    if (stats && stmt.groupBy.length > 0) {
      // For single column GROUP BY, use ndistinct
      const groupCols = stmt.groupBy.map(g => typeof g === 'string' ? g : g.name).filter(Boolean);
      if (groupCols.length === 1 && stats.columns[groupCols[0]]) {
        groupEstimate = stats.columns[groupCols[0]].distinct;
      } else if (groupCols.length > 1) {
        // Multi-column: product of ndistinct (capped at total rows)
        groupEstimate = groupCols.reduce((prod, c) => {
          return prod * (stats.columns[c]?.distinct || 10);
        }, 1);
        groupEstimate = Math.min(groupEstimate, estRows);
      }
    }
    plan.push({ operation: 'HASH_GROUP_BY', columns: stmt.groupBy, estimated_groups: groupEstimate });
  }

  // HAVING
  if (stmt.having) {
    plan.push({ operation: 'FILTER', condition: 'HAVING' });
  }

  // Window functions
  if (db._columnsHaveWindow(stmt.columns)) {
    plan.push({ operation: 'WINDOW_FUNCTION' });
  }

  // Aggregates
  if (stmt.columns.some(c => c.type === 'aggregate') && !stmt.groupBy) {
    plan.push({ operation: 'AGGREGATE' });
  }

  // ORDER BY
  if (stmt.orderBy) {
    if (db._canEliminateSort(stmt)) {
      plan.push({ operation: 'SORT_ELIMINATED', reason: 'BTree PK ordering', columns: stmt.orderBy.map(o => `${o.column} ${o.direction}`) });
    } else {
      plan.push({ operation: 'SORT', columns: stmt.orderBy.map(o => `${o.column} ${o.direction}`) });
    }
  }

  // DISTINCT
  if (stmt.distinct) {
    plan.push({ operation: 'DISTINCT' });
  }

  // LIMIT
  if (stmt.limit) {
    plan.push({ operation: 'LIMIT', count: stmt.limit });
  }

  return db._formatPlan(plan, format, stmt);
}

export function explainCompiled(db, stmt) {
  if (stmt.type !== 'SELECT') {
    return { type: 'COMPILED_PLAN', message: 'Only SELECT queries can be compiled' };
  }

  const engine = new CompiledQueryEngine(this);
  
  const plan = engine.planner.plan(stmt);
  const explainText = engine.explainCompiled(stmt);
  
  // Also check if it would actually compile
  const tableStats = engine.planner.getStats(stmt.from?.table);
  const wouldCompile = (tableStats?.rowCount || 0) >= 50;
  
  const lines = explainText.split('\n');
  lines.push('');
  lines.push(`Compilation: ${wouldCompile ? 'YES (table has ' + (tableStats?.rowCount || 0) + ' rows)' : 'NO (table too small)'}`);
  
  if (plan.joins?.length > 0) {
    lines.push(`Join strategies: ${plan.joins.map(j => j.type).join(', ')}`);
  }
  
  const aggInfo = engine._extractAggregation?.(stmt);
  if (aggInfo) {
    lines.push(`Aggregation: compiled (${aggInfo.aggregates.map(a => a.fn).join(', ')} with ${aggInfo.groupBy.length} group columns)`);
  }

  return {
    type: 'COMPILED_PLAN',
    plan: lines,
    message: lines.join('\n'),
    compiled: wouldCompile,
    estimatedCost: plan.estimatedCost || plan.totalCost,
  };
}

export function explainAnalyze(db, stmt) {
  // Build tree-structured plan with estimates
  let planTree = null;
  try {
    const builder = new PlanBuilder(this);
    planTree = builder.buildPlan(stmt);
  } catch (e) {
    // Plan builder may fail — fall through to legacy
  }

  // Get planner estimates (legacy)
  let plannerEstimate = null;
  try {
    const planner = new QueryPlanner(this);
    plannerEstimate = planner.plan(stmt);
  } catch (e) {
    // Planner may fail for complex queries — proceed with execution only
  }

  // Execute the actual query with timing and I/O tracking
  const startTime = performance.now();
  
  // Track I/O statistics: heap scans, buffer reads, index lookups
  const ioStats = { heapScans: 0, bufferReads: 0, indexLookups: 0, rowsExamined: 0 };
  
  // Instrument the table to count heap scans
  const ioTableName = stmt.from?.table;
  let origScan = null;
  let origGet = null;
  if (ioTableName && db.tables.has(ioTableName)) {
    const table = db.tables.get(ioTableName);
    if (table.heap && table.heap.scan) {
      origScan = table.heap.scan.bind(table.heap);
      const origScanFn = table.heap.scan;
      table.heap.scan = function(...args) {
        ioStats.heapScans++;
        const iter = origScan(...args);
        // Wrap iterator to count rows
        return {
          [Symbol.iterator]() {
            const it = iter[Symbol.iterator] ? iter[Symbol.iterator]() : iter;
            return {
              next() {
                const result = it.next();
                if (!result.done) {
                  ioStats.bufferReads++;
                  ioStats.rowsExamined++;
                }
                return result;
              }
            };
          }
        };
      };
    }
    if (table.heap && table.heap.get) {
      origGet = table.heap.get.bind(table.heap);
      table.heap.get = function(...args) {
        ioStats.bufferReads++;
        ioStats.indexLookups++;
        return origGet(...args);
      };
    }
  }
  
  let result;
  try {
    result = db._select(stmt);
  } finally {
    // Restore original methods
    if (ioTableName && db.tables.has(ioTableName)) {
      const table = db.tables.get(ioTableName);
      if (origScan) table.heap.scan = origScan;
      if (origGet) table.heap.get = origGet;
    }
  }
  
  const executionTime = performance.now() - startTime;
  const actualRows = result.rows.length;

  // Fill in actuals on the tree plan
  if (planTree) {
    // Set actuals on root node
    planTree.setActuals(actualRows, executionTime);
    // Propagate scan-level actuals
    fillScanActuals(db, planTree, stmt, actualRows);
  }

  // Build analyze output
  const analysis = [];
  
  // Table scan info
  const tableName = stmt.from?.table;
  if (tableName && db.tables.has(tableName)) {
    const table = db.tables.get(tableName);
    const totalRows = table.heap.tupleCount || 0;
    
    const engine = table.heap instanceof BTreeTable ? 'btree' : 'heap';
    
    // Use _estimateFilteredRows for better WHERE clause estimates
    let outputEstimate = plannerEstimate?.estimatedRows || totalRows;
    if (stmt.where) {
      const filterEst = db._estimateFilteredRows(tableName, stmt.where, totalRows);
      if (filterEst) outputEstimate = filterEst.estimated;
    }
    
    // For GROUP BY queries, estimate output rows based on group cardinality
    if (stmt.groupBy) {
      const stats = db._tableStats?.get(tableName);
      if (stats) {
        const groupCols = stmt.groupBy.map(g => typeof g === 'string' ? g : g.name).filter(Boolean);
        if (groupCols.length === 1 && stats.columns[groupCols[0]]) {
          outputEstimate = stats.columns[groupCols[0]].distinct;
        }
      }
    }

    analysis.push({
      operation: plannerEstimate?.scanType || 'TABLE_SCAN',
      table: tableName,
      engine,
      estimated_rows: outputEstimate,
      actual_rows: actualRows,
      total_table_rows: totalRows,
      selectivity: totalRows > 0 ? (actualRows / totalRows).toFixed(4) : '?',
    });

    if (plannerEstimate?.indexColumn) {
      analysis[0].index = plannerEstimate.indexColumn;
    }
  }

  // Join info
  for (const join of stmt.joins || []) {
    const joinTable = join.table?.table || join.table;
    analysis.push({
      operation: 'JOIN',
      table: joinTable,
      type: join.joinType || 'INNER',
    });
  }

  // WHERE filter
  if (stmt.where) {
    analysis.push({ operation: 'FILTER', actual_rows_after: actualRows });
  }

  // GROUP BY
  if (stmt.groupBy) {
    // Estimate group cardinality from ANALYZE stats
    let estimatedGroups = null;
    if (tableName && db._tableStats?.has(tableName)) {
      const stats = db._tableStats.get(tableName);
      const groupCols = stmt.groupBy.map(g => typeof g === 'string' ? g : g.name).filter(Boolean);
      if (groupCols.length === 1 && stats.columns[groupCols[0]]) {
        estimatedGroups = stats.columns[groupCols[0]].distinct;
      }
    }
    analysis.push({ operation: 'GROUP_BY', groups: actualRows, estimated_groups: estimatedGroups });
  }

  // ORDER BY
  if (stmt.orderBy) {
    if (db._canEliminateSort(stmt)) {
      analysis.push({ operation: 'SORT_ELIMINATED', reason: 'BTree PK ordering', actual_rows: actualRows });
    } else {
      analysis.push({ operation: 'SORT', rows_sorted: actualRows });
    }
  }

  const analyzeResult = {
    type: 'ROWS',
    rows: [
      ...analysis.map(a => {
        let line = a.operation;
        if (a.table) line += ` on ${a.table}`;
        if (a.engine) line += ` (engine=${a.engine})`;
        const parts = [];
        if (a.estimated_rows !== undefined) parts.push(`est=${a.estimated_rows}`);
        if (a.actual_rows !== undefined) parts.push(`actual=${a.actual_rows}`);
        if (a.total_table_rows !== undefined) parts.push(`total=${a.total_table_rows}`);
        if (a.selectivity) parts.push(`sel=${a.selectivity}`);
        if (a.index) parts.push(`index=${a.index}`);
        if (a.cost !== undefined) parts.push(`cost=${a.cost.toFixed(1)}`);
        if (parts.length) line += `  (${parts.join(', ')})`;
        return { 'QUERY PLAN': line };
      }),
      { 'QUERY PLAN': '' },
      { 'QUERY PLAN': `Planning Time: ${(plannerEstimate ? 0.1 : 0).toFixed(3)} ms` },
      { 'QUERY PLAN': `Execution Time: ${executionTime.toFixed(3)} ms` },
      { 'QUERY PLAN': `Rows Returned: ${actualRows}` },
      { 'QUERY PLAN': `Engine: ${analysis[0]?.engine || 'heap'}` },
      { 'QUERY PLAN': `Buffers: heap_scans=${ioStats.heapScans} buffer_reads=${ioStats.bufferReads} index_lookups=${ioStats.indexLookups} rows_examined=${ioStats.rowsExamined}` },
    ],
    analysis,
    ioStats,
    execution_time_ms: parseFloat(executionTime.toFixed(3)),
    actual_rows: actualRows,
    planTree: planTree || null,
    planTreeText: planTree ? PlanFormatter.format(planTree, { analyze: true }) : null,
  };
  
  // Add Volcano plan tree with per-operator instrumentation
  try {
    const rawPlan = volcanoBuildPlan(stmt, db.tables, db._indexes, db._tableStats);
    if (rawPlan) {
      const instrumented = instrumentPlan(rawPlan);
      // Execute through instrumented plan to collect real timings
      instrumented.open();
      let volcanoRows = 0;
      while (instrumented.next() !== null) volcanoRows++;
      instrumented.close();
      
      const timingTree = instrumented.explain(0);
      analyzeResult.rows.push({ 'QUERY PLAN': '' });
      analyzeResult.rows.push({ 'QUERY PLAN': 'Volcano ANALYZE (per-operator actual timing):' });
      for (const line of timingTree.split('\n')) {
        analyzeResult.rows.push({ 'QUERY PLAN': '  ' + line });
      }
      analyzeResult.rows.push({ 'QUERY PLAN': `  Total Volcano rows: ${volcanoRows}` });
      analyzeResult.volcanoAnalyze = { timingTree, volcanoRows };
    }
  } catch (e) {
    // Volcano instrumentation failed — add static plan tree as fallback
    try {
      const volcanoTree = volcanoExplainPlan(stmt, db.tables, db._indexes, db._tableStats);
      if (volcanoTree) {
        analyzeResult.rows.push({ 'QUERY PLAN': '' });
        analyzeResult.rows.push({ 'QUERY PLAN': 'Volcano Plan:' });
        for (const line of volcanoTree.split('\n')) {
          analyzeResult.rows.push({ 'QUERY PLAN': '  ' + line });
        }
      }
    } catch (e2) { /* skip if Volcano can't handle this query */ }
  }
  
  return analyzeResult;
}

export function fillScanActuals(db, node, stmt, totalActualRows) {
  // Walk the tree and fill in scan-level actuals where we can
  if (node.type === 'Seq Scan' && node.table) {
    const table = db.tables.get(node.table);
    if (table) {
      const tableRows = table.heap?._rowCount || table.heap?.tupleCount || 0;
      node.setActuals(tableRows, 0); // Scan reads all rows
    }
  }
  for (const child of node.children) {
    fillScanActuals(db, child, stmt, totalActualRows);
  }
}

