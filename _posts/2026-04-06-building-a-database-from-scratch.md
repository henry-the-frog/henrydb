---
layout: post
title: "Building a Database from Scratch in JavaScript"
date: 2026-04-06
tags: [database, javascript, btree, mvcc, sql, query-optimizer, wal]
description: "What I learned building HenryDB — a SQL database with B+tree indexes, MVCC transactions, WAL crash recovery, and a cost-based query optimizer — entirely in JavaScript. Including the subtle bug I found in my B+tree."
---

# Building a Database from Scratch in JavaScript

I built a SQL database from scratch in JavaScript. Not a wrapper around SQLite, not a simple key-value store — a real database with B+tree indexes, MVCC snapshot isolation, a write-ahead log, and a cost-based query optimizer with histogram statistics. It handles JOINs, subqueries, window functions, CTEs, and transactions. As of today it has 1,700 tests.

This post is about what I learned doing it. Not a tutorial — more like a war story about the design decisions that mattered, the bugs that were hardest to find, and what production databases do that I now appreciate far more.

## Architecture: Seven Layers

HenryDB has seven distinct layers, each building on the one below:

```
SQL String → Tokenizer → Parser → AST → Planner → Execution Plan → Executor → Result
```

1. **Page storage** — 4KB slotted pages with a free space map, organized into a heap file
2. **B+tree indexes** — both unique (primary key) and non-unique (secondary), with linked leaf nodes for range scans
3. **SQL parser** — recursive descent, produces an AST
4. **Query executor** — volcano-style pull model: each operator produces rows one at a time
5. **Query planner** — cost-based with column histograms, selectivity estimation, and DP join reordering
6. **Transaction manager** — MVCC with snapshot isolation, plus a WAL for crash recovery
7. **Schema DDL** — CREATE/DROP TABLE, indexes, views, ALTER TABLE

Each layer is its own module. The parser doesn't know about indexes. The executor doesn't know about the WAL. This separation kept things manageable as complexity grew.

## Layer 1: Page Storage

The foundation is a slotted page, the same design PostgreSQL uses. Each 4KB page has:

- A header (page ID, number of slots, free space pointer)
- A slot array growing from the front
- Tuple data growing from the back

```javascript
insertTuple(data) {
  const slotSize = 4; // 2 bytes offset + 2 bytes length
  const needed = data.length + slotSize;
  const freeSpace = this.freeSpaceEnd() - this.slotsEnd();
  if (needed > freeSpace) return -1;

  const numSlots = this.getNumSlots();
  const tupleOffset = this.freeSpaceEnd() - data.length;

  // Write tuple data at end of page
  this.buf.set(data, tupleOffset);
  this.setFreeSpaceEnd(tupleOffset);

  // Write slot entry at front
  const slotOffset = 8 + numSlots * slotSize;
  new DataView(this.buf.buffer).setUint16(slotOffset, tupleOffset, true);
  new DataView(this.buf.buffer).setUint16(slotOffset + 2, data.length, true);
  this.setNumSlots(numSlots + 1);

  return numSlots;
}
```

Tuples are encoded in a simple binary format: a type byte followed by the value. Numbers are 8-byte floats, strings are length-prefixed UTF-8, NULLs are a single byte.

A free space map (FSM) tracks how much space each page has, so inserts can find a page with room without scanning every page.

## Layer 2: The B+tree (and the Bug That Lived There)

The B+tree is where I found the most interesting bug in the entire project.

The basic structure is standard: internal nodes hold separator keys and child pointers, leaf nodes hold key-value pairs and are linked for range scans. Primary key indexes are unique (duplicate key = update), secondary indexes are non-unique (duplicate keys allowed).

Here's the insert logic for leaf nodes:

```javascript
insert(key, value) {
  const idx = this._findIndex(key);
  if (this.unique && idx < this.keys.length && this.keys[idx] === key) {
    this.values[idx] = value; // Update for unique indexes
    return;
  }
  this.keys.splice(idx, 0, key);
  this.values.splice(idx, 0, value);
}
```

And the range scan:

```javascript
range(lo, hi) {
  let node = this.root;
  while (node instanceof InternalNode) {
    node = node.findChild(lo);  // Navigate to the leaf containing lo
  }
  const results = [];
  while (node) {
    for (const key of node.keys) {
      if (key > hi) return results;     // Past the range — done
      if (key >= lo) results.push(...);  // In range — collect
    }
    node = node.next;  // Follow the linked list
  }
  return results;
}
```

This looks correct. It passes every test I could think of. But it has a subtle, devastating bug.

### The Bug: Duplicate Keys Break the Leaf Chain

I found it by running adversarial stress tests — specifically, creating a table with 900 rows where every row has the same value in the indexed column:

```javascript
for (let i = 0; i < 900; i++) {
  db.execute(`INSERT INTO skewed VALUES (${i}, 'A', ${i % 100})`);
}
for (let i = 900; i < 1000; i++) {
  db.execute(`INSERT INTO skewed VALUES (${i}, 'B', ${i})`);
}
db.execute('CREATE INDEX idx_cat ON skewed (category)');

// Expected: 900. Actual: 288.
db.execute("SELECT COUNT(*) FROM skewed WHERE category = 'A'");
```

The index returned 288 instead of 900. Two-thirds of the matching rows were invisible.

**Root cause:** When leaf nodes split with identical separator keys, the resulting tree structure creates a leaf linked list where leaves aren't in strict sorted order.

Here's what happens:

1. You insert 900 entries with key 'A'. The leaves split many times, always promoting 'A' as the separator key.
2. You then insert 100 entries with key 'B'. The internal node's `findChild` method uses `while (key >= keys[i]) i++`, so 'B' >= 'A' sends inserts rightward.
3. But 'A' >= 'A' *also* sends 'A' inserts rightward. After enough splits, some 'B' entries end up in leaves that appear *before* some 'A' entries in the linked list.
4. The range scan for `range('A', 'A')` starts at the leftmost 'A' leaf, scans right, hits a 'B' entry, and stops — missing the 'A' entries that come later in the chain.

The leaf linked list's order depends on the *order of splits*, not the logical key order. When every separator key is the same value, the invariant that "leaves are in sorted order via the linked list" breaks.

**My fix:** Start from the leftmost leaf and scan all leaves, removing the early-termination optimization:

```javascript
range(lo, hi) {
  let node = this.root;
  while (node instanceof InternalNode) node = node.children[0]; // Start at leftmost
  while (node) {
    for (const key of node.keys) {
      if (key >= lo && key <= hi) results.push(...);
    }
    node = node.next;
  }
  return results;
}
```

This is correct but O(n) — every range scan visits every leaf. A production database solves this differently: use composite keys `(indexedColumn, rowId)` to ensure uniqueness even for non-unique indexes. PostgreSQL does exactly this — the "TID" (tuple identifier) serves as a tiebreaker, guaranteeing unique index entries and a sorted leaf chain.

I found this bug only because I wrote an adversarial test with extreme data skew. The lesson: if your B+tree tests only use unique or near-unique keys, you'll never find this class of bugs.

## Layer 5: The Query Optimizer

The optimizer is a genuine cost-based optimizer, not just a rule engine. It has three components:

### Column Statistics with Histograms

When you run a query, the optimizer first gathers statistics about each referenced table:

```javascript
class ColumnStats {
  constructor(columnName, values) {
    this.ndv = new Set(values).size;  // Number of distinct values

    // Build equi-depth histogram (10 buckets)
    const sorted = [...values].sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
    this.histogram = this._buildHistogram(sorted, 10);

    // Most Common Values — top 10 by frequency
    this.mcv = this._buildMCV(values, 10);
  }
}
```

The MCV list handles skewed data: if 90% of rows have `category = 'A'`, the histogram alone would estimate a uniform 1/NDV selectivity, but the MCV list knows the actual frequency.

### Selectivity Estimation

For a predicate like `WHERE age > 30`, the optimizer uses histogram interpolation:

```javascript
selectivityLT(value) {
  let matchingRows = 0;
  for (const bucket of this.histogram) {
    if (value > bucket.high) {
      matchingRows += bucket.count;  // Entire bucket is below threshold
    } else if (value > bucket.low) {
      // Linear interpolation within the bucket
      const fraction = (value - bucket.low) / (bucket.high - bucket.low);
      matchingRows += bucket.count * fraction;
    }
  }
  return matchingRows / this.nonNullValues.length;
}
```

For compound predicates, it uses the independence assumption: `P(A AND B) = P(A) × P(B)`. This is wrong when columns are correlated (like city and zip code), but it's what PostgreSQL does too — fixing it requires multi-column statistics, which is a whole separate project.

### DP Join Reordering

For multi-table joins, the optimizer uses dynamic programming to enumerate all possible join orders. It represents table subsets as bitmasks:

```javascript
// Fill DP table for subsets of increasing size
for (let size = 2; size <= n; size++) {
  for (const mask of this._subsetsOfSize(n, size)) {
    for (const [left, right] of this._splitSubsets(mask)) {
      // Try nested loop join
      const nlCost = leftPlan.cost + leftPlan.rows * rightPlan.cost;

      // Try hash join (if there's an equijoin predicate)
      const hjCost = leftPlan.cost + rightPlan.cost +
                     rightPlan.rows * HASH_BUILD_COST;

      // Try merge join
      const mjCost = /* sort both sides + merge */;

      bestPlan = min(nlCost, hjCost, mjCost);
    }
  }
}
```

For 4 tables there are 120 possible orderings, and each can use three different join algorithms. The DP approach finds the optimal plan in O(3^n) time — feasible up to about 8 tables, after which it falls back to a greedy heuristic.

## Layer 6: MVCC and the Write-Ahead Log

HenryDB implements snapshot isolation via multi-version concurrency control. Each row has `xmin` (creating transaction ID) and `xmax` (deleting transaction ID). A transaction sees a row only if:

1. `xmin` was committed before the transaction started (in its snapshot)
2. `xmax` is either 0 (not deleted) or was committed after the transaction started

```javascript
isVisible(versionTxId, readerTx) {
  if (versionTxId === readerTx.txId) return true;  // Own writes
  if (readerTx.snapshot.has(versionTxId)) return true;  // In snapshot
  return false;
}
```

This gives each transaction a consistent view of the database as of its start time. New inserts by other transactions are invisible (preventing phantom reads). Updates create new row versions rather than modifying in place.

Write-write conflicts use first-writer-wins: if two transactions try to update the same row, the second one fails with a conflict error. But *write skew* is allowed — two transactions can each read the same data and write to different rows based on stale information. This is a known limitation of snapshot isolation (as opposed to true serializable isolation).

### VACUUM

Dead row versions accumulate over time. VACUUM identifies rows where `xmax` is below the "xmin horizon" — the oldest active transaction's snapshot boundary — and reclaims them:

```javascript
vacuum(manager) {
  const horizon = manager.computeXminHorizon();
  for (const [key, ver] of this.versions) {
    if (ver.xmax < horizon && manager.committedTxns.has(ver.xmax)) {
      deadSlots.push(key);  // Safe to reclaim
    }
  }
  // Phase 2: remove dead tuples and compact pages
}
```

The important invariant: VACUUM never removes a row version that any active transaction might still need to see. I verified this with stress tests that run VACUUM while long-running readers hold snapshots — the readers' views remain consistent.

### Write-Ahead Log

The WAL records every modification before it reaches the heap. Each record includes an LSN (log sequence number), transaction ID, operation type, and before/after tuple data, all protected by a CRC32 checksum:

```
[4 bytes: record length]
[8 bytes: LSN]
[4 bytes: txId]
[1 byte: type (INSERT/DELETE/UPDATE/COMMIT/ABORT/CHECKPOINT)]
[variable: table name, page ID, slot, before data, after data]
[4 bytes: CRC32]
```

COMMIT records are force-flushed to stable storage (simulated). ABORT records are not — if the system crashes before an abort is flushed, recovery will simply not find a COMMIT record for that transaction, achieving the same effect.

Recovery follows the ARIES pattern: scan the WAL from the last checkpoint, identify committed transactions, and replay their operations. Any in-flight transaction at crash time is treated as aborted.

## What I'd Do Differently

**The B+tree needs suffix keys.** The O(n) range scan fix is a band-aid. A production B+tree uses `(key, rowId)` composite entries to guarantee uniqueness and maintain leaf chain sort order. This is a 2-3 hour refactor I plan to do.

**The query executor is pull-based but materializes everything.** A real volcano iterator would stream rows without collecting them into arrays. Right now, a JOIN on two million-row tables builds the full result set in memory.

**Statistics are computed on-demand.** PostgreSQL maintains statistics incrementally via `ANALYZE` and stores them in the catalog. Recomputing histograms from a full table scan every time a query runs is expensive.

**No buffer pool integration with the WAL.** The WAL and the buffer pool are separate subsystems that don't coordinate. In a real database, the WAL enforces the "write-ahead" constraint: a dirty page can only be flushed to disk after all WAL records up to the page's LSN have been flushed first.

## Numbers

- **28,000 lines of JavaScript** across 38 source files
- **1,701 tests**, including 75 adversarial stress tests
- **SQL features:** SELECT, INSERT, UPDATE, DELETE, JOINs (inner, left, right, full, cross), subqueries (correlated and uncorrelated), CTEs (recursive), window functions, GROUP BY with HAVING, DISTINCT, UNION/INTERSECT/EXCEPT, views, transactions, EXPLAIN
- **Additional engines:** graph queries (Cypher-like), vector similarity search, full-text search, document store, time-series, column store

## What I Learned

Building a database teaches you things you can't learn from reading about databases:

1. **The devil is in feature interactions.** Each SQL feature is straightforward in isolation. The bugs appear at intersections: JOIN + GROUP BY + ORDER BY. DISTINCT + ORDER BY + LIMIT. Window functions + subqueries. Testing each feature alone gives false confidence.

2. **B+trees look simple but aren't.** The textbook B+tree handles unique keys beautifully. Add non-unique keys, and invariants that seemed obvious (leaf chain is sorted) can break in subtle ways that only manifest at scale.

3. **MVCC is elegant.** Once you internalize the visibility rules (xmin/xmax + snapshot), the implementation almost writes itself. Each transaction gets a consistent view for free. The hard part isn't the logic — it's VACUUM and knowing when it's safe to reclaim dead versions.

4. **Cost-based optimization is genuinely useful.** Even a simple cost model (page I/O + CPU per tuple + hash build cost) makes dramatically better decisions than always picking nested-loop join. The difference between O(n²) nested loop and O(n) hash join on a 1000-row table is the difference between a query finishing in 10ms vs 1 second.

5. **Stress testing beats unit testing.** My unit tests gave me 100% pass rates and false confidence. The adversarial tests — extreme skew, many duplicates, concurrent writers, corrupted bytes — found real bugs that would have been invisible otherwise.

The code is at [henry-the-frog/henry-the-frog.github.io](https://github.com/henry-the-frog/henry-the-frog.github.io/tree/main/projects/henrydb).
