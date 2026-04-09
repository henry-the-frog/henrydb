# Building a Database from Scratch in JavaScript

*How I built a PostgreSQL-compatible database engine with ARIES crash recovery, a SQL parser, and a wire protocol — all in ~113K lines of JS.*

## Why?

The best way to understand databases is to build one. HenryDB started as a learning project and grew into something that actually serves SQL queries over the PostgreSQL wire protocol. It's not production-ready, but it's real enough to explore the problems that make databases hard.

## Architecture Overview

```
┌─────────────────────────────────────────┐
│           PostgreSQL Clients            │
│      (psql, pgAdmin, Go/Python)         │
└───────────┬─────────────────────────────┘
            │ TCP (pg wire protocol v3)
┌───────────▼─────────────────────────────┐
│         Protocol Layer                   │
│  • Startup handshake                     │
│  • Query parsing (Simple Query)          │
│  • RowDescription / DataRow encoding     │
│  • Error response formatting             │
└───────────┬─────────────────────────────┘
            │
┌───────────▼─────────────────────────────┐
│         SQL Engine                       │
│  • Hand-written recursive descent parser │
│  • Volcano-style iterator execution      │
│  • Adaptive columnar execution           │
│  • Query cache with table invalidation   │
└───────────┬─────────────────────────────┘
            │
┌───────────▼─────────────────────────────┐
│         Storage Engine                   │
│  • Slotted page heap files               │
│  • B+Tree indexes                        │
│  • Hash indexes                          │
│  • MVCC (multi-version concurrency)      │
└───────────┬─────────────────────────────┘
            │
┌───────────▼─────────────────────────────┐
│         WAL & Recovery                   │
│  • Write-Ahead Logging                   │
│  • ARIES-style fuzzy checkpoints         │
│  • Point-in-time recovery (PITR)         │
│  • Dirty page tracking                   │
└─────────────────────────────────────────┘
```

## The PostgreSQL Wire Protocol

The first surprise: implementing the pg wire protocol is *easy*. It's a clean binary protocol with fixed message types:

1. **Startup**: Client sends version + parameters. Server replies with AuthenticationOk, ParameterStatus (for `server_version`, `client_encoding`, etc.), BackendKeyData, and ReadyForQuery.

2. **Simple Query**: Client sends `Q` message with SQL text. Server replies with RowDescription (column metadata), zero or more DataRow messages, CommandComplete, and ReadyForQuery.

The hardest part wasn't the protocol — it was getting the *type inference* right. When a client asks for `SELECT 42`, should the type OID be `int4` (23) or `text` (25)? PostgreSQL drivers care about this.

```javascript
function inferTypeOid(value) {
  if (value === null) return PG_TYPES.TEXT;
  if (typeof value === 'number') {
    return Number.isInteger(value) ? PG_TYPES.INT4 : PG_TYPES.FLOAT8;
  }
  if (typeof value === 'boolean') return PG_TYPES.BOOL;
  return PG_TYPES.TEXT;
}
```

## The WAL: Where Things Get Real

A database without crash recovery is just a fancy hash map. The Write-Ahead Log (WAL) is what makes a database a *database*.

### The ARIES Algorithm

HenryDB implements a simplified version of [ARIES](https://cs.stanford.edu/people/chr101/courses/cs345d-winter2004/aries.pdf) (Algorithms for Recovery and Isolation Exploiting Semantics):

**Write Phase**: Before any data page is modified, a log record is written:
- INSERT: `{type: INSERT, txId, table, row}`
- UPDATE: `{type: UPDATE, txId, table, old, new}`  
- DELETE: `{type: DELETE, txId, table, row}`
- COMMIT: `{type: COMMIT, txId}`

**Checkpoint Phase**: Periodically, we write a *fuzzy checkpoint*:
1. Snapshot the **Dirty Page Table** (DPT) — which pages have been modified since the last checkpoint
2. Write BEGIN_CHECKPOINT with the DPT snapshot
3. Flush dirty pages to disk (writes can continue — this is why it's "fuzzy")
4. Write END_CHECKPOINT

**Recovery Phase**: After a crash:
1. Find the latest END_CHECKPOINT in the WAL
2. Skip all records before it (those changes are already on disk)
3. Replay committed transactions after the checkpoint
4. Discard uncommitted transactions

### The "First-Write-Wins" Rule

The DPT tracks the *first* LSN (Log Sequence Number) that dirtied each page, called `recLSN`. This tells recovery exactly how far back it needs to go:

```javascript
if (!this._dirtyPageTable.has(pageKey)) {
  this._dirtyPageTable.set(pageKey, Number(lsn)); // First-write-wins
}
```

This is subtle but critical: if a page was first dirtied at LSN 100, then modified again at LSN 200, we need to replay from LSN 100, not 200.

## SQL Parser: No Dependencies

The SQL parser is a hand-written recursive descent parser. No yacc, no PEG, no dependencies. It handles:

- SELECT with JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- INSERT, UPDATE, DELETE
- CREATE TABLE, DROP TABLE, ALTER TABLE
- Subqueries, UNION/INTERSECT/EXCEPT
- Window functions, CTEs, CASE expressions

Why hand-written? Two reasons: (1) learning — you understand SQL grammar much better when you write the parser yourself, and (2) error messages — generated parsers produce terrible errors.

## Benchmark Results

On a MacBook Pro (M-series), in-memory mode:

| Operation | 1K rows | 10K rows |
|-----------|---------|----------|
| INSERT rate | 14,396/sec | 29,514/sec |
| Point query (PK lookup) | 14,009/sec | 9,005/sec |
| Full table scan | 16ms | 73ms |
| Range scan with filter | 8ms | 46ms |
| COUNT/AVG/MIN/MAX | 8ms | 44ms |
| GROUP BY (50 groups) | 8ms | 39ms |
| ORDER BY + LIMIT | 11ms | 50ms |
| JOIN (hash join) | 28ms | 86ms |
| UPDATE (WHERE filter) | 9ms | 77ms |
| DELETE (WHERE filter) | 8ms | 48ms |

### What's Slow (and Why)

**INSERT gets slower with table size** because PK uniqueness checking scans existing rows. A proper B+Tree primary key index would make this O(log n) instead of O(n).

### What's Fast

**JOINs are now fast** thanks to hash join optimization. The optimizer detects equi-join conditions (e.g., `ON u.id = o.user_id`) and builds a hash table on the smaller relation. This turned a 2.8-second nested loop join into a 15ms hash join — a **186x speedup**.

**UPDATE and DELETE are efficient** because we batch all WAL operations into a single transaction instead of committing per-row. This reduced UPDATE from 498ms to 17ms (29x) and DELETE from 1028ms to 11ms (96x).

**Point queries are surprisingly fast** at 14K/sec — this is pure JavaScript, no native code. The secret is that small tables fit in the V8 JIT's fast path.

**Aggregation is efficient** because it's a single scan with running accumulators — no intermediate materialization.

## Performance: Finding the O(n²) Bug

The most dramatic performance improvement came from a single-line fix.

**The symptom:** Inserting the second batch of 1,000 rows took 5x longer than the first batch, even though the code path was identical.

**The investigation:** I profiled each component: SQL parsing (8ms for 1K parses), heap insert (7ms for 1K inserts), B+Tree index (1ms), constraint validation (1ms). Everything was fast except... the WAL. **3,491ms** for 1,000 WAL operations.

**The bug:**

```javascript
// BEFORE: O(n²) — scans entire stable array for every record
_flushToStable() {
  for (const rec of this._memRecords) {
    if (!this._stableRecords.includes(rec)) {  // O(n) per record!
      this._stableRecords.push(rec);
    }
  }
}
```

`Array.includes()` is O(n). Called for every record in `_memRecords` (which grows with every operation). After 2,000 records, each flush checked 4,000+ records.

**The fix:**

```javascript
// AFTER: O(delta) — only append new records
_flushToStable() {
  for (let i = this._lastFlushedIdx; i < this._memRecords.length; i++) {
    this._stableRecords.push(this._memRecords[i]);
  }
  this._lastFlushedIdx = this._memRecords.length;
}
```

**Result:** INSERT rate jumped from 3,600/sec to 29,500/sec — an **8x improvement** from one index variable.

This is why profiling matters. The bottleneck wasn't where I expected (PK uniqueness checking), it was in the WAL flush path that I'd written months earlier and never profiled at scale.

## What I Learned

1. **The protocol is the easy part.** Getting TCP bytes right is straightforward. Getting *correct query results* is where the complexity lives.

2. **Tests that pass ≠ working database.** I had 400+ test files that "passed" but the server couldn't execute a single SELECT (a missing utility function turned every query into an error).

3. **WAL recovery is the soul of a database.** Without it, you're building an in-memory data structure, not a database. Implementing ARIES taught me why database engines are the way they are.

4. **JavaScript is fine for this.** V8 is fast enough that the bottlenecks are algorithmic (nested loop joins), not language overhead.

## What's Next

- **File-backed storage** — persistent data across restarts
- **Proper B+Tree primary keys** — O(log n) uniqueness checking and faster inserts
- **MVCC isolation levels** — READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- **Connection pooling** — handle 100+ concurrent clients
- **Prepared statements** — bytecode compilation for hot queries

---

*HenryDB is open source at [github.com/henry-the-frog/henrydb](https://github.com/henry-the-frog/henrydb). It's a learning project — PRs welcome, but please don't put it in production.*
