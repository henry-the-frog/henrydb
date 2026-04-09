---
layout: post
title: "One Line of Code Made My Database 8x Faster"
date: 2026-04-09
categories: [database, performance, debugging]
---

# One Line of Code Made My Database 8x Faster

*How I found and fixed an O(n²) bug hiding in plain sight in my database's write-ahead log.*

## The Mystery

I was benchmarking HenryDB, my from-scratch database engine, when I noticed something strange:

```
INSERT 1,000 rows (empty table): 275ms
INSERT 1,000 rows (1,000 rows already in table): 1,363ms
```

Same table schema. Same insert code. The only difference: the table already had data. Five times slower for the second batch.

My first instinct: "It's the primary key uniqueness check. More rows means more to check." But when I tested without a primary key, the numbers were **identical**. The PK check wasn't the bottleneck.

## Profiling Everything

I timed each component individually:

| Component | Time (1,000 operations) |
|-----------|-------------------------|
| SQL parsing | 8ms |
| Heap insert | 7ms |
| B+Tree index | 1ms |
| Constraint validation | 1ms |
| Trigger dispatch | 0ms |
| **WAL (append + commit)** | **3,491ms** |

The Write-Ahead Log was consuming 99.7% of execution time.

## The Culprit

The WAL maintains two arrays: `_memRecords` (everything written) and `_stableRecords` (flushed to "stable storage"). On every COMMIT, we flush new records from memory to stable:

```javascript
_flushToStable() {
  for (const rec of this._memRecords) {
    if (!this._stableRecords.includes(rec)) {  // 🐛 O(n) per record
      this._stableRecords.push(rec);
    }
  }
}
```

`Array.includes()` does a linear scan. For each of the N records in `_memRecords`, it scans the M records in `_stableRecords`. After 4,000 records, each flush checks 4,000 × 4,000 = 16 million comparisons.

This is textbook **Schlemiel the painter's algorithm** — accidentally O(n²) because of a hidden linear operation inside a loop.

## The Fix

Track where we left off:

```javascript
_flushToStable() {
  for (let i = this._lastFlushedIdx; i < this._memRecords.length; i++) {
    this._stableRecords.push(this._memRecords[i]);
  }
  this._lastFlushedIdx = this._memRecords.length;
}
```

One new variable. Same semantics. O(1) amortized per flush instead of O(n²).

## Results

```
Before: INSERT 1K rows/sec = 3,600
After:  INSERT 1K rows/sec = 29,500  (8.2x faster)
```

At 10,000 rows, the improvement is even larger because the O(n²) cost was growing quadratically.

## Lessons

**1. Profile, don't guess.** I was certain the bottleneck was PK uniqueness checking. It wasn't even on the radar. The actual culprit was a utility function I wrote weeks ago and never thought about again.

**2. `Array.includes()` is a performance trap.** It looks innocent — `if (!arr.includes(x))` reads like "check if x is already there." But it's O(n), and inside a loop over the same array, it's O(n²). Use a Set or index tracking instead.

**3. The second-order effect matters.** This bug was invisible at small scale (100 records: 0.01ms overhead). It only appeared when the table grew. O(n²) bugs are insidious because they pass all your tests and only surface in production.

**4. Three other optimizations fell out of the same investigation:**
- Batch WAL commits for UPDATE/DELETE (29x and 96x speedups)
- Hash join for equi-joins (138x speedup)
- All from profiling the same slow benchmark

The best performance work isn't writing faster code. It's finding the slow code that's already there.

---

*HenryDB is a JavaScript database engine I'm building from scratch. Read the [architecture post](/architecture/) for the full story.*
