# Vectorized Execution for HenryDB (Apr 24, 2026)

## Current Architecture
- Volcano (iterator) model: `open()`, `next()`, `close()`
- Each `next()` returns one row
- Operators: SeqScan, Filter, Project, HashJoin, NestedLoopJoin, MergeJoin, Sort, Aggregate, etc.
- ~1870 lines total (volcano.js + planner)

## Vectorized Execution Overview
Instead of processing one row at a time, process batches (vectors) of rows.
Key insight: Amortizes per-row overhead, enables SIMD-like processing in JS.

## Strategies

### 1. Column-at-a-time (MonetDB style)
- Store each column as a typed array (Float64Array, Int32Array)
- Operators process entire columns
- Pros: Cache-friendly, simple, great for aggregations
- Cons: Materialization overhead for complex queries, hard for hash joins

### 2. Vectorized batches (DuckDB style)
- Process batches of N rows (e.g., 1024)
- Each batch is a set of column vectors
- Operators: `nextBatch()` instead of `next()`
- Pros: Balanced between volcano and columnar
- Cons: More complex operator logic

### 3. Compiled pipelines (Hyper/Umbra style)
- Generate specialized code per query
- Fuse operators into tight loops
- Pros: Fastest possible execution
- Cons: Compilation overhead, hard to implement in JS

## Recommended Approach for HenryDB
**Option 2: Vectorized batches** — Best fit because:
1. Existing volcano operators can be adapted (add `nextBatch()` alongside `next()`)
2. JS typed arrays provide some columnar benefit
3. No compilation needed
4. Can coexist with existing volcano model (gradual migration)

## Design Sketch
```javascript
class VectorBatch {
  constructor(schema, capacity = 1024) {
    this.columns = schema.map(col => ({
      name: col.name,
      data: new Array(capacity),  // or Float64Array for numeric
      nulls: new Uint8Array(capacity),
    }));
    this.size = 0;
    this.capacity = capacity;
  }
}

class VSeqScan {
  nextBatch() {
    const batch = new VectorBatch(this.schema);
    while (batch.size < batch.capacity && this.pos < this.rows.length) {
      const row = this.rows[this.pos++];
      for (let c = 0; c < batch.columns.length; c++) {
        batch.columns[c].data[batch.size] = row[batch.columns[c].name];
      }
      batch.size++;
    }
    return batch.size > 0 ? batch : null;
  }
}

class VFilter {
  nextBatch() {
    while (true) {
      const batch = this.child.nextBatch();
      if (!batch) return null;
      // Evaluate predicate on entire batch → selection vector
      const sel = new Uint32Array(batch.size);
      let selSize = 0;
      for (let i = 0; i < batch.size; i++) {
        if (this.predicate(batch, i)) sel[selSize++] = i;
      }
      if (selSize > 0) return compact(batch, sel, selSize);
    }
  }
}
```

## Performance Expectations
- SeqScan + Filter: 2-5x improvement (branch prediction, reduced function call overhead)
- Aggregation: 3-10x improvement (tight loops over typed arrays)
- Hash Join: 2-3x improvement (batch probing)
- Complex queries: 2-4x overall

## Next Steps
1. Implement VectorBatch data structure
2. Implement VSeqScan and VFilter
3. Benchmark against current volcano on aggregation queries
4. If promising, extend to HashJoin and Aggregate
