// cost-model.js — Cost estimation for volcano operators
// Each operator estimates: rows (cardinality), cost (relative), ioOps (I/O operations)

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

function _estimate(node, stats) {
  const type = node.constructor.name;
  const desc = node.describe();
  
  switch (type) {
    case 'SeqScan': {
      const tableName = desc.details.table;
      const tableInfo = stats.get(tableName) || stats.get(tableName?.toLowerCase());
      const rows = tableInfo ? (tableInfo.rowCount ?? 1000) : 1000; // Use stats if available, else default
      return { type, rows, cost: rows, ioOps: Math.ceil(rows / 100) };
    }
    
    case 'IndexScan': {
      const rows = 10; // Assume selective
      return { type, rows, cost: Math.log2(rows + 1) * 2, ioOps: Math.ceil(Math.log2(rows + 1)) };
    }
    
    case 'Filter': {
      const child = _estimate(desc.children[0], stats);
      // Default selectivity: 1/3
      const selectivity = 0.33;
      const rows = Math.max(1, Math.ceil(child.rows * selectivity));
      return { type, rows, cost: child.cost + rows, ioOps: child.ioOps };
    }
    
    case 'Project': {
      const child = _estimate(desc.children[0], stats);
      return { type, rows: child.rows, cost: child.cost + child.rows * 0.1, ioOps: child.ioOps };
    }
    
    case 'Limit': {
      const child = _estimate(desc.children[0], stats);
      const limit = desc.details.limit || 10;
      const rows = Math.min(limit, child.rows);
      // Cost is greatly reduced for pipelined operators
      return { type, rows, cost: rows + 1, ioOps: Math.ceil(rows / 100) };
    }
    
    case 'Distinct': {
      const child = _estimate(desc.children[0], stats);
      // Assume 50% distinct
      return { type, rows: Math.ceil(child.rows * 0.5), cost: child.cost + child.rows, ioOps: child.ioOps };
    }
    
    case 'Sort': {
      const child = _estimate(desc.children[0], stats);
      const sortCost = child.rows * Math.log2(child.rows + 1);
      return { type, rows: child.rows, cost: child.cost + sortCost, ioOps: child.ioOps };
    }
    
    case 'HashJoin': {
      const left = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const right = desc.children[1] ? _estimate(desc.children[1], stats) : { rows: 100, cost: 100, ioOps: 1 };
      // Build: hash the smaller side. Probe: scan the larger side.
      const buildCost = right.cost + right.rows * 1.5; // hash overhead
      const probeCost = left.cost + left.rows; // scan + hash lookup
      const rows = Math.ceil(left.rows * right.rows * 0.1); // Assume 10% match rate
      return { type, rows, cost: buildCost + probeCost, ioOps: left.ioOps + right.ioOps };
    }
    
    case 'NestedLoopJoin': {
      const outer = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const inner = desc.children[1] ? _estimate(desc.children[1], stats) : { rows: 100, cost: 100, ioOps: 1 };
      const rows = Math.ceil(outer.rows * inner.rows * 0.1);
      return { type, rows, cost: outer.cost + outer.rows * inner.cost, ioOps: outer.ioOps + outer.rows * inner.ioOps };
    }
    
    case 'IndexNestedLoopJoin': {
      const outer = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      // Index lookup is O(log N) per outer row
      const rows = outer.rows; // Assume 1:1 match
      const lookupCost = Math.log2(1000) * outer.rows;
      return { type, rows, cost: outer.cost + lookupCost, ioOps: outer.ioOps + outer.rows };
    }
    
    case 'MergeJoin': {
      const left = desc.children[0] ? _estimate(desc.children[0], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const right = desc.children[1] ? _estimate(desc.children[1], stats) : { rows: 1000, cost: 1000, ioOps: 10 };
      const rows = Math.ceil((left.rows + right.rows) * 0.1);
      return { type, rows, cost: left.cost + right.cost + left.rows + right.rows, ioOps: left.ioOps + right.ioOps };
    }
    
    case 'HashAggregate': {
      const child = _estimate(desc.children[0], stats);
      // Groups: assume 10% of input
      const groups = desc.details.groupBy === 'none' ? 1 : Math.max(1, Math.ceil(child.rows * 0.1));
      return { type, rows: groups, cost: child.cost + child.rows * 1.2, ioOps: child.ioOps };
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
