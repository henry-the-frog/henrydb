# HenryDB Query Optimizer Cost Model Design

uses: 0
created: 2026-04-18
tags: database, henrydb, optimizer, cost-model, statistics

## Current State
The optimizer uses simple heuristics:
- If an index exists for a WHERE column, use index scan
- Otherwise, sequential scan
- No cost comparison, no selectivity estimation, no join reordering

## Proposed: ANALYZE Command
```sql
ANALYZE table_name;
```
Gathers and stores:
- Total row count
- Per-column: ndistinct, null fraction, most common values (top-10), histogram (10 buckets)

### Implementation
```js
// In db.js: _analyze(tableName)
const stats = { rowCount: 0, columns: {} };
const sample = []; // Reservoir sample of 1000 rows

for (const row of table.heap.scan()) {
  stats.rowCount++;
  // Collect values per column
}

for (const col of table.schema) {
  const values = sample.map(r => r[col.name]);
  stats.columns[col.name] = {
    ndistinct: new Set(values.filter(v => v != null)).size,
    nullFraction: values.filter(v => v == null).length / values.length,
    // mostCommon: top-10 by frequency
    // histogram: 10 equal-width buckets
  };
}

table._stats = stats;
```

## Proposed: Selectivity Estimation
```js
function estimateSelectivity(condition, stats) {
  if (condition.op === '=') return 1 / stats.ndistinct;
  if (condition.op === '<') return fractionFromHistogram(condition.value, stats.histogram);
  if (condition.op === 'BETWEEN') return (high - low) / range;
  // AND: multiply, OR: add minus intersection
}
```

## Proposed: Cost Comparison
```js
const seqScanCost = stats.rowCount * CPU_TUPLE_COST;
const idxScanCost = Math.log2(stats.rowCount) * CPU_INDEX_COST + selectivity * stats.rowCount * CPU_TUPLE_COST;
const useIndex = idxScanCost < seqScanCost;
```

## Implementation Order
1. ANALYZE command (gather stats)
2. Store stats in table metadata
3. Selectivity estimation for = and range predicates
4. Cost-based index vs scan decision
5. Join order optimization (later, more complex)

## Estimated Effort
- ANALYZE: ~100 lines
- Selectivity: ~50 lines
- Cost comparison: ~30 lines
- Total: ~200 lines for basic cost-based optimization
