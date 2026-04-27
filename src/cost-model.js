// cost-model.js — Parametric cost estimation for query execution
// Uses PostgreSQL-style cost parameters for I/O and CPU costs.
// Supports engine-specific cost estimation (volcano, codegen, vectorized).

// Default cost parameters (configurable via setCostParams)
let SEQ_PAGE_COST = 1.0;       // sequential page read
let RANDOM_PAGE_COST = 4.0;    // random page read (index lookup)
let CPU_TUPLE_COST = 0.01;     // process one row
let CPU_INDEX_TUPLE_COST = 0.005; // process one index entry
let CPU_OPERATOR_COST = 0.0025;   // evaluate one predicate
let HASH_BUILD_COST = 0.02;      // hash one tuple (build phase)
let DEFAULT_PAGE_SIZE = 100;      // rows per page (for I/O estimation)

// Engine-specific cost multipliers
// These model the overhead/efficiency of each execution engine
const ENGINE_COSTS = {
  volcano: {
    // Baseline: per-tuple virtual function calls, iterator protocol overhead
    cpuMultiplier: 1.0,       // reference point
    startupCost: 0,           // negligible startup
    perTupleOverhead: 0.005,  // virtual dispatch per tuple
    batchSize: 1,             // processes one row at a time
    description: 'Pull-based iterator model',
  },
  codegen: {
    // Compiled pipeline: eliminates virtual dispatch, tight inner loops
    cpuMultiplier: 0.2,       // 5x faster CPU work (JIT-compiled tight loops)
    startupCost: 5.0,         // compilation overhead (proportional to plan complexity)
    perTupleOverhead: 0.0005, // minimal per-tuple overhead (no virtual dispatch)
    batchSize: 1,             // still row-at-a-time, but compiled
    description: 'JIT-compiled pipeline',
  },
  vectorized: {
    // Batch processing: SIMD-friendly, good cache utilization
    cpuMultiplier: 0.3,       // 3x faster CPU work (batch operations)
    startupCost: 1.0,         // materialization setup
    perTupleOverhead: 0.001,  // amortized batch overhead
    batchSize: 1024,          // processes in batches
    description: 'Vectorized batch processing',
  },
};

/**
 * Set cost model parameters (e.g., from SET commands).
 */
export function setCostParams(params) {
  if (params.seqPageCost != null) SEQ_PAGE_COST = params.seqPageCost;
  if (params.randomPageCost != null) RANDOM_PAGE_COST = params.randomPageCost;
  if (params.cpuTupleCost != null) CPU_TUPLE_COST = params.cpuTupleCost;
  if (params.cpuIndexTupleCost != null) CPU_INDEX_TUPLE_COST = params.cpuIndexTupleCost;
  if (params.cpuOperatorCost != null) CPU_OPERATOR_COST = params.cpuOperatorCost;
}

/**
 * Get current cost parameters.
 */
export function getCostParams() {
  return { SEQ_PAGE_COST, RANDOM_PAGE_COST, CPU_TUPLE_COST, CPU_INDEX_TUPLE_COST, CPU_OPERATOR_COST };
}

/**
 * Compute cost estimates for a volcano iterator tree.
 * Walks the tree recursively, annotating each node with estimates.
 * 
 * @param {Iterator} root — root of the iterator tree
 * @param {Map} [tableStats] — optional table statistics {tableName → {rowCount, avgRowSize}}
 * @returns {object} — {rows, cost, ioOps, plan: string}
 */
export function estimateCost(root, tableStats = new Map()) {
  return _estimate(root, tableStats);
}

/**
 * Estimate cost for all execution engines and return comparative analysis.
 * 
 * @param {Iterator} root — root of the iterator tree
 * @param {Map} [tableStats] — optional table statistics
 * @returns {object} — { volcano: {cost, rows}, codegen: {cost, rows, compileCost}, vectorized: {cost, rows}, cheapest: string }
 */
export function estimateMultiEngineCost(root, tableStats = new Map()) {
  const baseCost = _estimate(root, tableStats);
  const results = {};
  
  for (const [engine, params] of Object.entries(ENGINE_COSTS)) {
    const cpuCost = baseCost.cost * params.cpuMultiplier;
    const tupleOverhead = baseCost.rows * params.perTupleOverhead;
    const totalCost = params.startupCost + cpuCost + tupleOverhead;
    
    results[engine] = {
      cost: totalCost,
      rows: baseCost.rows,
      ioOps: baseCost.ioOps,
      startupCost: params.startupCost,
      executionCost: cpuCost + tupleOverhead,
      description: params.description,
    };
  }
  
  // Find cheapest engine
  let cheapest = 'volcano';
  let cheapestCost = results.volcano.cost;
  for (const [engine, data] of Object.entries(results)) {
    if (data.cost < cheapestCost) {
      cheapest = engine;
      cheapestCost = data.cost;
    }
  }
  
  results.cheapest = cheapest;
  results.baseCost = baseCost;
  return results;
}

/**
 * Get engine cost parameters (for inspection/testing).
 */
export function getEngineCosts() {
  return { ...ENGINE_COSTS };
}

/**
 * Override engine cost parameters.
 */
export function setEngineCosts(engine, params) {
  if (ENGINE_COSTS[engine]) {
    Object.assign(ENGINE_COSTS[engine], params);
  }
}

function _estimate(node, stats) {
  const type = node.constructor.name;
  const desc = node.describe();
  
  switch (type) {
    case 'SeqScan': {
      const tableName = desc.details.table;
      const tableInfo = stats.get(tableName) || stats.get(tableName?.toLowerCase());
      const rows = tableInfo?.rowCount || 1000;
      const pages = Math.ceil(rows / DEFAULT_PAGE_SIZE);
      const ioCost = pages * SEQ_PAGE_COST;
      const cpuCost = rows * CPU_TUPLE_COST;
      return { type, rows, cost: ioCost + cpuCost, ioOps: pages };
    }
    
    case 'IndexScan': {
      const rows = 10; // Assume selective
      const treeHeight = Math.ceil(Math.log2(rows + 1));
      const ioCost = treeHeight * RANDOM_PAGE_COST;
      const cpuCost = rows * CPU_INDEX_TUPLE_COST + rows * CPU_TUPLE_COST;
      return { type, rows, cost: ioCost + cpuCost, ioOps: treeHeight };
    }
    
    case 'Filter': {
      const child = _estimate(desc.children[0], stats);
      const selectivity = 0.33;
      const rows = Math.max(1, Math.ceil(child.rows * selectivity));
      // CPU cost: evaluate predicate on every input row
      const filterCost = child.rows * CPU_OPERATOR_COST;
      return { type, rows, cost: child.cost + filterCost, ioOps: child.ioOps };
    }
    
    case 'Project': {
      const child = _estimate(desc.children[0], stats);
      return { type, rows: child.rows, cost: child.cost + child.rows * CPU_OPERATOR_COST, ioOps: child.ioOps };
    }
    
    case 'Limit': {
      const child = _estimate(desc.children[0], stats);
      const limit = desc.details.limit || 10;
      const rows = Math.min(limit, child.rows);
      // Early termination: only pay fraction of child cost proportional to rows fetched
      const fraction = child.rows > 0 ? rows / child.rows : 1;
      const cost = child.cost * fraction + rows * CPU_TUPLE_COST;
      return { type, rows, cost, ioOps: Math.ceil(child.ioOps * fraction) };
    }
    
    case 'Distinct': {
      const child = _estimate(desc.children[0], stats);
      // Assume 50% distinct
      return { type, rows: Math.ceil(child.rows * 0.5), cost: child.cost + child.rows, ioOps: child.ioOps };
    }
    
    case 'Sort': {
      const child = _estimate(desc.children[0], stats);
      // N log N comparison-based sort
      const sortCost = child.rows * Math.log2(child.rows + 1) * CPU_OPERATOR_COST;
      return { type, rows: child.rows, cost: child.cost + sortCost, ioOps: child.ioOps };
    }
    
    case 'HashJoin': {
      const left = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const right = desc.children[1] ? _estimate(desc.children[1], stats) : { rows: 100, cost: 100, ioOps: 1 };
      // Build: hash the smaller side (right). Probe: scan the larger side.
      const buildCost = right.cost + right.rows * HASH_BUILD_COST;
      const probeCost = left.cost + left.rows * CPU_TUPLE_COST;
      const rows = Math.ceil(left.rows * right.rows * 0.1);
      return { type, rows, cost: buildCost + probeCost, ioOps: left.ioOps + right.ioOps };
    }
    
    case 'NestedLoopJoin': {
      const outer = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const inner = desc.children[1] ? _estimate(desc.children[1], stats) : { rows: 100, cost: 100, ioOps: 1 };
      const rows = Math.ceil(outer.rows * inner.rows * 0.1);
      // Outer full scan + for each outer row, full inner scan
      return { type, rows, cost: outer.cost + outer.rows * inner.cost, ioOps: outer.ioOps + outer.rows * inner.ioOps };
    }
    
    case 'IndexNestedLoopJoin': {
      const outer = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const rows = outer.rows;
      // Each outer row does one index lookup
      const lookupCost = outer.rows * (Math.log2(1000) * RANDOM_PAGE_COST + CPU_INDEX_TUPLE_COST);
      return { type, rows, cost: outer.cost + lookupCost, ioOps: outer.ioOps + outer.rows };
    }
    
    case 'MergeJoin': {
      const left = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const right = desc.children[1] ? _estimate(desc.children[1], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const rows = Math.ceil((left.rows + right.rows) * 0.1);
      // Linear scan of both sorted inputs
      const mergeCost = (left.rows + right.rows) * CPU_TUPLE_COST;
      return { type, rows, cost: left.cost + right.cost + mergeCost, ioOps: left.ioOps + right.ioOps };
    }
    
    case 'HashAggregate': {
      const child = _estimate(desc.children[0], stats);
      const groups = desc.details.groupBy === 'none' ? 1 : Math.max(1, Math.ceil(child.rows * 0.1));
      const aggCost = child.rows * (HASH_BUILD_COST + CPU_OPERATOR_COST);
      return { type, rows: groups, cost: child.cost + aggCost, ioOps: child.ioOps };
    }
    
    case 'Window': {
      const child = _estimate(desc.children[0], stats);
      return { type, rows: child.rows, cost: child.cost + child.rows * 2, ioOps: child.ioOps };
    }
    
    case 'Union': {
      const left = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 100, cost: 100, ioOps: 1 };
      const right = desc.children[1] ? _estimate(desc.children[1], stats) : { rows: 100, cost: 100, ioOps: 1 };
      return { type, rows: left.rows + right.rows, cost: left.cost + right.cost, ioOps: left.ioOps + right.ioOps };
    }
    
    default: {
      // Try to estimate from children
      if (desc.children && desc.children.length > 0) {
        const child = _estimate(desc.children[0], stats);
        return { type, rows: child.rows, cost: child.cost, ioOps: child.ioOps };
      }
      return { type, rows: 1, cost: 1, ioOps: 0 };
    }
  }
}

/**
 * Format cost estimation as a string for EXPLAIN output.
 */
export function formatCostEstimate(root, tableStats) {
  const est = estimateCost(root, tableStats);
  return `Estimated: ${est.rows} rows, cost=${est.cost.toFixed(0)}, I/O=${est.ioOps}`;
}

/**
 * Generate EXPLAIN with cost annotations.
 */
export function explainWithCost(root, tableStats = new Map(), indent = 0) {
  const desc = root.describe();
  const est = estimateCost(root, tableStats);
  const prefix = '  '.repeat(indent);
  const details = Object.entries(desc.details || {})
    .filter(([, v]) => v != null)
    .map(([k, v]) => `${k}=${v}`)
    .join(', ');
  
  let line = `${prefix}→ ${desc.type}`;
  if (details) line += ` (${details})`;
  line += `  [rows=${est.rows} cost=${est.cost.toFixed(0)}]`;
  
  const lines = [line];
  for (const child of desc.children || []) {
    lines.push(explainWithCost(child, tableStats, indent + 1));
  }
  return lines.join('\n');
}
