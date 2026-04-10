// join-optimizer.js — Cost-Based Join Optimizer
//
// Estimates costs for three join strategies and picks the cheapest:
//   1. Nested Loop Join (NLJ)  — O(n*m), good when inner table is small or indexed
//   2. Hash Join (HJ)          — O(n+m), good for equi-joins on large tables
//   3. Sort-Merge Join (SMJ)   — O(n log n + m log m), good when both inputs are sorted
//
// Cost model considers:
//   - CPU cost (comparisons, hashing)
//   - I/O cost (page reads — dominant for disk-based DBs)
//   - Memory cost (hash table size, sort buffers)
//
// References:
//   - Selinger et al., "Access Path Selection in a Relational Database Management System" (1979)
//   - PostgreSQL: src/backend/optimizer/path/costsize.c

/**
 * TableStats — statistics about a table, gathered by ANALYZE.
 */
export class TableStats {
  constructor(options = {}) {
    this.name = options.name || 'unknown';
    this.rows = options.rows || 0;           // Total row count (cardinality)
    this.pages = options.pages || 0;         // Pages on disk
    this.avgRowWidth = options.avgRowWidth || 100; // Bytes per row
    this.distinctValues = options.distinctValues || new Map(); // col → distinct count
    this.indexes = options.indexes || [];     // [{column, type: 'btree'|'hash'}]
    this.sorted = options.sorted || null;     // Column name if table is sorted by it
  }

  /** Selectivity of a column: fraction of distinct values (1/NDV). */
  selectivity(column) {
    const ndv = this.distinctValues.get(column);
    if (!ndv || ndv === 0) return 1;
    return 1 / ndv;
  }

  /** Estimated output rows for an equi-join with another table. */
  joinCardinality(other, joinCol) {
    const ndvA = this.distinctValues.get(joinCol) || this.rows;
    const ndvB = other.distinctValues.get(joinCol) || other.rows;
    const maxNdv = Math.max(ndvA, ndvB);
    // Formula: |A| × |B| / max(NDV(A.col), NDV(B.col))
    return maxNdv > 0 ? (this.rows * other.rows) / maxNdv : 0;
  }

  /** Check if column has a B-tree index. */
  hasIndex(column) {
    return this.indexes.some(idx => idx.column === column);
  }

  /** Check if table is sorted by a column. */
  isSortedBy(column) {
    return this.sorted === column;
  }
}

// ============================================================
// Cost Constants (tunable)
// ============================================================
const COSTS = {
  SEQ_PAGE_READ:    1.0,    // Cost of sequential page read
  RANDOM_PAGE_READ: 4.0,    // Cost of random page read (seek + read)
  CPU_COMPARISON:   0.01,   // Cost of one comparison
  CPU_HASH:         0.02,   // Cost of hashing one value
  CPU_SORT_KEY:     0.05,   // Cost of processing one sort key
  MEMORY_PER_ROW:   0.001,  // Cost of keeping one row in memory
};

/**
 * CostEstimate — estimated cost of a query plan.
 */
export class CostEstimate {
  constructor(strategy, options = {}) {
    this.strategy = strategy;
    this.cpuCost = options.cpuCost || 0;
    this.ioCost = options.ioCost || 0;
    this.memoryCost = options.memoryCost || 0;
    this.outputRows = options.outputRows || 0;
    this.details = options.details || '';
  }

  get totalCost() {
    return this.cpuCost + this.ioCost + this.memoryCost;
  }

  toString() {
    return `${this.strategy}: total=${this.totalCost.toFixed(1)} (cpu=${this.cpuCost.toFixed(1)}, io=${this.ioCost.toFixed(1)}, mem=${this.memoryCost.toFixed(1)}) → ${this.outputRows} rows [${this.details}]`;
  }
}

// ============================================================
// Join Cost Estimators
// ============================================================

/**
 * Nested Loop Join cost.
 * For each row in outer, scan ALL rows in inner.
 * With index on inner: for each outer row, do index lookup.
 */
export function costNestedLoopJoin(outer, inner, joinCol) {
  const outputRows = outer.joinCardinality(inner, joinCol);
  let cpuCost, ioCost, details;

  if (inner.hasIndex(joinCol)) {
    // Index NLJ: outer scan + index probe per outer row
    ioCost = outer.pages * COSTS.SEQ_PAGE_READ +           // Scan outer
             outer.rows * 2 * COSTS.RANDOM_PAGE_READ;       // ~2 random reads per index probe
    cpuCost = outer.rows * COSTS.CPU_COMPARISON +            // One comparison per outer row
              outputRows * COSTS.CPU_COMPARISON;              // Match comparisons
    details = `Index NLJ: scan ${outer.name} (${outer.rows} rows), index probe ${inner.name}`;
  } else {
    // Simple NLJ: for each outer row, scan entire inner
    ioCost = outer.pages * COSTS.SEQ_PAGE_READ +            // Scan outer once
             outer.rows * inner.pages * COSTS.SEQ_PAGE_READ; // Scan inner per outer row
    cpuCost = outer.rows * inner.rows * COSTS.CPU_COMPARISON;
    details = `Simple NLJ: scan ${outer.name} × scan ${inner.name} (${outer.rows} × ${inner.rows})`;
  }

  return new CostEstimate('Nested Loop Join', {
    cpuCost,
    ioCost,
    memoryCost: 0, // NLJ uses constant memory
    outputRows: Math.round(outputRows),
    details,
  });
}

/**
 * Hash Join cost.
 * Phase 1 (build): scan inner, build hash table.
 * Phase 2 (probe): scan outer, probe hash table.
 * Memory: hash table for inner.
 */
export function costHashJoin(outer, inner, joinCol) {
  const outputRows = outer.joinCardinality(inner, joinCol);

  // Always build hash table on the smaller side
  const [build, probe] = inner.rows <= outer.rows ? [inner, outer] : [outer, inner];

  const ioCost = build.pages * COSTS.SEQ_PAGE_READ +    // Scan build side
                 probe.pages * COSTS.SEQ_PAGE_READ;       // Scan probe side

  const cpuCost = build.rows * COSTS.CPU_HASH +           // Hash build rows
                  probe.rows * COSTS.CPU_HASH +            // Hash probe rows
                  outputRows * COSTS.CPU_COMPARISON;        // Match comparisons

  const memoryCost = build.rows * COSTS.MEMORY_PER_ROW;    // Hash table in memory

  return new CostEstimate('Hash Join', {
    cpuCost,
    ioCost,
    memoryCost,
    outputRows: Math.round(outputRows),
    details: `Build hash on ${build.name} (${build.rows} rows), probe ${probe.name} (${probe.rows} rows)`,
  });
}

/**
 * Sort-Merge Join cost.
 * Phase 1: Sort both inputs by join column (if not already sorted).
 * Phase 2: Merge the sorted streams.
 */
export function costSortMergeJoin(outer, inner, joinCol) {
  const outputRows = outer.joinCardinality(inner, joinCol);

  // Sort costs (skip if already sorted)
  const sortOuterCost = outer.isSortedBy(joinCol) ? 0
    : outer.rows * Math.log2(outer.rows) * COSTS.CPU_SORT_KEY;
  const sortInnerCost = inner.isSortedBy(joinCol) ? 0
    : inner.rows * Math.log2(inner.rows) * COSTS.CPU_SORT_KEY;

  const ioCost = outer.pages * COSTS.SEQ_PAGE_READ +      // Scan outer
                 inner.pages * COSTS.SEQ_PAGE_READ;         // Scan inner

  const cpuCost = sortOuterCost + sortInnerCost +
                  (outer.rows + inner.rows) * COSTS.CPU_COMPARISON; // Merge

  const sortMemory = outer.isSortedBy(joinCol) ? 0 : outer.rows * COSTS.MEMORY_PER_ROW;
  const memoryCost = sortMemory + (inner.isSortedBy(joinCol) ? 0 : inner.rows * COSTS.MEMORY_PER_ROW);

  let details = 'Sort-Merge:';
  if (!outer.isSortedBy(joinCol)) details += ` sort ${outer.name}`;
  if (!inner.isSortedBy(joinCol)) details += ` sort ${inner.name}`;
  details += ` merge (${outer.rows} + ${inner.rows} rows)`;

  return new CostEstimate('Sort-Merge Join', {
    cpuCost,
    ioCost,
    memoryCost,
    outputRows: Math.round(outputRows),
    details,
  });
}

// ============================================================
// Join Optimizer
// ============================================================

/**
 * JoinOptimizer — pick the best join strategy based on cost estimation.
 */
export class JoinOptimizer {
  constructor(options = {}) {
    this.costs = { ...COSTS, ...options.costs };
  }

  /**
   * Choose the best join strategy for two tables.
   * Returns { best: CostEstimate, alternatives: CostEstimate[] }
   */
  optimize(tableA, tableB, joinCol) {
    const alternatives = [
      costNestedLoopJoin(tableA, tableB, joinCol),
      costNestedLoopJoin(tableB, tableA, joinCol), // Try both orderings
      costHashJoin(tableA, tableB, joinCol),
      costSortMergeJoin(tableA, tableB, joinCol),
    ];

    // Sort by total cost
    alternatives.sort((a, b) => a.totalCost - b.totalCost);

    return {
      best: alternatives[0],
      alternatives,
    };
  }

  /**
   * Multi-table join ordering using dynamic programming (Selinger-style).
   * For N tables, finds the optimal join order.
   * Complexity: O(2^N * N^2) — practical for up to ~15 tables.
   */
  optimizeMultiJoin(tables, joinConditions) {
    const n = tables.length;
    if (n === 1) return { plan: tables[0].name, cost: 0 };

    // DP table: best plan for each subset of tables
    const dp = new Map(); // bitmask → {cost, plan, outputStats}

    // Base cases: single tables
    for (let i = 0; i < n; i++) {
      const mask = 1 << i;
      dp.set(mask, {
        cost: tables[i].pages * COSTS.SEQ_PAGE_READ, // Seq scan cost
        plan: tables[i].name,
        stats: tables[i],
      });
    }

    // Build up subsets of increasing size
    for (let size = 2; size <= n; size++) {
      for (let mask = 0; mask < (1 << n); mask++) {
        if (popcount(mask) !== size) continue;

        let bestCost = Infinity;
        let bestPlan = null;
        let bestStats = null;

        // Try all ways to split this subset into two non-empty halves
        for (let sub = (mask - 1) & mask; sub > 0; sub = (sub - 1) & mask) {
          const other = mask ^ sub;
          if (!dp.has(sub) || !dp.has(other)) continue;

          const left = dp.get(sub);
          const right = dp.get(other);

          // Find join condition between left and right subsets
          const joinCol = this._findJoinCol(tables, sub, other, joinConditions);
          if (!joinCol) continue;

          // Estimate join cost
          const joinCost = costHashJoin(left.stats, right.stats, joinCol);
          const totalCost = left.cost + right.cost + joinCost.totalCost;

          if (totalCost < bestCost) {
            bestCost = totalCost;
            bestPlan = `(${left.plan} ⋈[${joinCol}] ${right.plan})`;
            bestStats = new TableStats({
              name: bestPlan,
              rows: joinCost.outputRows,
              pages: Math.ceil(joinCost.outputRows * 100 / 4096), // Estimate
            });
          }
        }

        if (bestPlan) {
          dp.set(mask, { cost: bestCost, plan: bestPlan, stats: bestStats });
        }
      }
    }

    const fullMask = (1 << n) - 1;
    const result = dp.get(fullMask);
    return result || { plan: 'Unable to find join order', cost: Infinity };
  }

  _findJoinCol(tables, maskA, maskB, joinConditions) {
    for (const cond of joinConditions) {
      const idxA = tables.findIndex(t => t.name === cond.tableA);
      const idxB = tables.findIndex(t => t.name === cond.tableB);
      if (idxA === -1 || idxB === -1) continue;
      const bitA = 1 << idxA;
      const bitB = 1 << idxB;
      if ((maskA & bitA) && (maskB & bitB)) return cond.column;
      if ((maskA & bitB) && (maskB & bitA)) return cond.column;
    }
    return null;
  }
}

function popcount(n) {
  let count = 0;
  while (n) { count += n & 1; n >>= 1; }
  return count;
}

export { COSTS };
