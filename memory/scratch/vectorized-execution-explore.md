# Vectorized Execution in JS — Exploration Results (2026-04-22)

## Key Finding: Virtual Dispatch is the Bottleneck

In JavaScript, vectorized (batch-at-a-time) execution wins more from **reducing function call overhead** than from cache effects or SIMD. V8's JIT is already good at optimizing tight loops.

## Benchmarks (500K rows, median of 10 runs)

| Pipeline | Row-at-a-time | Vectorized | Speedup |
|----------|-------------|-----------|---------|
| SUM WHERE (columnar) | 1.48ms | 0.81ms | 1.8x |
| GROUP BY (hash agg) | 1.01ms | 0.92ms | 1.1x |
| Filter→Project→Agg (3 ops) | 11.68ms | 4.09ms | **2.85x** |
| 5-operator pipeline | 13.51ms | 2.33ms | **5.8x** |

## Why Vectorization Wins in JS

1. **Virtual dispatch**: each `next()` through N operators = N function calls per row. With 1024-row batches, it's N calls per 1024 rows.
2. **V8 JIT**: tight loops on typed arrays (Float64Array) get heavily optimized. The batched inner loop lets V8 see the hot path.
3. **Branch prediction**: processing all rows through one operator before moving to next improves prediction.

## Why Vectorization Can Lose in JS

1. **Object allocation**: creating batch arrays/objects per batch costs more than in-place iteration.
2. **Shallow pipelines**: with only 1-2 operators, dispatch overhead is small. The batch creation overhead dominates.
3. **Row-object format**: if data is stored as `{col1: val, col2: val}` objects, slicing them into batches copies more than iterating.

## Implications for HenryDB

- **Worth it for deep plans (3+ operators)** — common in real SQL queries
- **Requires columnar internal format** — row objects kill the benefit
- **Selection vectors > copying** — pass indices, not filtered arrays
- **Best approach**: hybrid — keep Volcano interface for correctness/composability, but internal operators process batches
- **GROUP BY doesn't benefit much** — bottleneck is hash table, not iteration

## Design Sketch: Hybrid Volcano-Vectorized

```
class VectorizedFilter extends Iterator {
  // Same open/next/close interface as Volcano
  // But internally buffers 1024 rows and processes as batch
  open() { this.child.open(); this.buffer = []; this.bufIdx = 0; }
  next() {
    while (this.bufIdx >= this.buffer.length) {
      // Refill buffer with next batch
      this.buffer = [];
      for (let i = 0; i < 1024; i++) {
        const row = this.child.next();
        if (row === null) break;
        if (this.pred(row)) this.buffer.push(row);
      }
      this.bufIdx = 0;
      if (this.buffer.length === 0) return null;
    }
    return this.buffer[this.bufIdx++];
  }
}
```

This is a **drop-in replacement** that maintains the Iterator protocol but gets batch processing benefits internally.

## HenryDB Overhead Profile (50K rows)

| Component | Time | % |
|-----------|------|---|
| Raw HeapFile scan | 29ms | 54% |
| SQL parse + plan + Volcano | 25ms | 46% |
| Total (db.execute) | 54ms | 100% |

Key insight: Even with the HeapFile being the dominant cost, there's still ~25ms of overhead from the SQL layer. Vectorized execution would reduce the Volcano portion. If vectorization gives 3-5x speedup on the iterator chain (as benchmarked), the Volcano overhead drops from 25ms to 5-8ms, making overall queries ~35ms (35% faster).

But the real win would be combining vectorization with a columnar storage format, which would reduce both the scan time AND the iterator overhead.
