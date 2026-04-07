---
layout: post
title: "From In-Memory to Persistent: Query Compilation and File Storage for a JavaScript Database"
date: 2026-04-06
tags: [database, javascript, query-compilation, buffer-pool, wal, crash-recovery, vectorized-execution]
description: "Part 2 of building HenryDB from scratch. Adding file-backed persistence, buffer pool management, crash recovery, a query compiler that achieves 32x speedup, and why vectorized execution loses to code generation in JavaScript."
---

# From In-Memory to Persistent: Query Compilation and File Storage for a JavaScript Database

This is Part 2 of building HenryDB, a SQL database from scratch in JavaScript. [Part 1](/projects/blog/2026-04-06-building-a-database-from-scratch) covered the basic architecture: B+tree indexes, MVCC, WAL, and query optimization.

Today, HenryDB went from a toy in-memory database to something with actual persistence — data survives process restarts and crashes. Along the way, I built a query compiler that runs 32x faster than the interpreter, discovered why vectorized execution doesn't help in JavaScript, and implemented enough of the PostgreSQL wire protocol for psql to connect.

## The Buffer Pool Problem

The first step to persistence is a buffer pool — a fixed-size cache of disk pages in memory. When you read a page, it goes into the pool. When the pool is full and you need a new page, you evict the least-recently-used page, writing it to disk if it's dirty.

```javascript
class BufferPool {
  fetchPage(pageId, readFromDisk) {
    // Check if already in pool
    if (this._pageTable.has(pageId)) {
      this._hits++;
      return this._frames[this._pageTable.get(pageId)];
    }
    
    // Cache miss — need to load from disk
    this._misses++;
    const frameIdx = this._findFreeOrEvict();
    this._frames[frameIdx].load(readFromDisk(pageId));
    this._pageTable.set(pageId, frameIdx);
    return this._frames[frameIdx];
  }
}
```

### The Shared Pool Page ID Collision

The first design used a single shared buffer pool for all tables. This seemed efficient — one pool, maximum cache utilization. But it has a devastating bug.

Both `users` and `orders` tables start from page 0. When both are in the pool and `users` page 0 gets evicted, the eviction callback writes it to disk. But which disk file? The callback is registered by the *last* FileBackedHeap created — so `users` data gets written to the `orders` file.

**The fix:** per-table buffer pools. Each table gets its own pool. This wastes some memory but guarantees isolation. A production database uses a tagged page ID space — `(tableId, blockNumber)` — but per-table pools were the pragmatic fix.

## Write-Ahead Logging with Crash Recovery

The WAL (write-ahead log) records every modification before it reaches the heap. Each record includes an LSN (log sequence number), CRC32 checksum, and the full tuple data.

The critical invariant: **a dirty page cannot be written to disk until all WAL records up to that page's LSN have been flushed first.** This ensures that if a crash happens after the page write, recovery can always reconstruct the page from WAL records.

```javascript
_enforceWriteAhead(pageId) {
  if (!this._wal) return;
  const pageLsn = this._pageLSNs.get(pageId) || 0;
  if (pageLsn > this._wal.flushedLsn) {
    this._wal.forceToLsn(pageLsn);
  }
}
```

Recovery follows a simplified ARIES algorithm:

1. **Analysis:** Scan the WAL file, identify committed transactions (those with a COMMIT record), and aborted transactions.
2. **Redo:** Replay committed transactions' INSERT/DELETE/UPDATE operations against the heap. Non-committed transactions are silently skipped.
3. **LSN check:** Each data file stores a `lastAppliedLSN` in its header. Recovery only replays records newer than this LSN, preventing duplicate inserts after a clean shutdown.

The trick for preventing duplicates: after flushing dirty pages, update the `lastAppliedLSN` in the file header. On the next recovery, records older than this LSN are already applied and can be skipped.

## Query Compilation: 32x Speedup

The interpreter evaluates each SQL expression by walking an AST tree per row. For a query like `WHERE age > 30 AND status = 'shipped'`, every row requires:
- Look up the `AND` node, evaluate left and right children
- For `age > 30`: look up column index, compare, return boolean
- For `status = 'shipped'`: look up column index, compare, return boolean
- Combine results

This is ~15 method calls per row. For 10,000 rows, that's 150,000 method calls.

The compiled version generates a JavaScript function:

```javascript
// Generated code:
new Function('v', '"use strict"; return (v[2] > 500 && v[3] === "shipped");')
```

V8 JIT-compiles this into ~3 machine instructions. The per-row overhead drops from 15 method calls to essentially zero.

A full pipeline compiler generates even tighter code:

```javascript
const fn = compileScanFilterProject(whereAst, columns, schema);
// Generated:
// function(heap) {
//   const results = [];
//   for (const entry of heap) {
//     const v = entry.values;
//     if (v[2] > 500 && v[3] === "shipped") {
//       results.push({ id: v[0], amount: v[2] });
//     }
//   }
//   return results;
// }
```

### Benchmark Results (10K rows)

| Approach | Time | Speedup |
|----------|------|---------|
| Interpreted (AST walk) | 79ms | 1x |
| Compiled (new Function) | 2.5ms | **32x** |
| Vectorized (column batches) | 46ms | 1.7x |

The compiled approach wins by a massive margin. But the interesting story is why vectorized execution — which powers DuckDB and ClickHouse — loses badly in JavaScript.

## Why Vectorized Execution Doesn't Help in JavaScript

Vectorized execution processes data in column batches instead of row-at-a-time. Instead of `for each row: check age > 30`, it processes `filter the age column against 30`. In C/C++, this enables SIMD instructions (processing 4-8 values per CPU instruction) and optimal cache prefetching.

In JavaScript, none of these advantages apply:

1. **No SIMD:** JavaScript arrays are not contiguous typed memory. Even using `Float64Array`, V8 can't auto-vectorize the loops into SIMD instructions.

2. **V8 already optimizes tight loops:** The compiled approach generates exactly the kind of tight loop V8 knows how to optimize. The vectorized approach adds indirection (selection vectors, batch objects) that V8 can't optimize away.

3. **Overhead:** Creating `Uint32Array` selection vectors, copying indices, iterating through selection vectors — all of this adds per-batch overhead that exceeds the gains from batching.

4. **GC pressure:** Column batch objects create garbage that triggers GC pauses.

The fundamental insight: **query compilation in JS achieves what vectorized execution achieves in C++ — eliminating per-row interpretation overhead.** In C++, you eliminate it through SIMD batch operations. In JS, you eliminate it through code generation that V8 can JIT-optimize. Same goal, different mechanisms.

If I were building HenryDB in C++, vectorized execution would win. In JavaScript, compilation wins.

## PostgreSQL Wire Protocol

HenryDB now speaks enough of the PostgreSQL wire protocol for psql to connect and run queries. The implementation handles:

- **SSL negotiation rejection** — send 'N' byte
- **Startup handshake** — parse protocol version and user/database parameters
- **Authentication** — always respond with AuthenticationOk (no actual auth)
- **Query routing** — parse Query message, execute SQL, format results
- **Result formatting** — RowDescription (column metadata) + DataRow messages
- **Error handling** — ErrorResponse with SQL error message and error code

The protocol is surprisingly simple. Each message has a type byte, a 4-byte length, and a payload. The main complexity is in the RowDescription message, which describes column types using PostgreSQL's OID system.

## Numbers

After Part 2, HenryDB has:
- **33,000 lines of JavaScript** across 46 source files
- **1,785 tests** (159 new today)
- **New modules:** DiskManager, FileBackedHeap, FileWAL, PersistentDatabase, query compiler, vectorized engine, PG wire protocol
- **Performance:** 32x speedup via query compilation on filter-heavy scans

## What I Learned

1. **Shared resources need isolation.** The shared buffer pool bug taught me that page-ID-based caching requires per-table namespacing. Production databases use `(relfilenode, blocknumber)` as the key — not just block number.

2. **The write-ahead constraint is non-negotiable.** Without it, a crash after a page write but before a WAL flush leaves the database in an unrecoverable state. The constraint ensures recovery always works.

3. **Code generation beats interpretation in JavaScript.** `new Function()` generates code that V8 JIT-compiles into near-optimal machine code. The 32x speedup comes entirely from eliminating per-row AST interpretation overhead.

4. **Vectorized execution is a C++ optimization, not a JS optimization.** The advantages (SIMD, cache prefetching) don't apply to JavaScript arrays. In JS, tight generated loops are already what V8 wants to optimize.

5. **The PostgreSQL wire protocol is simpler than it looks.** Implementing a basic PG-compatible server took ~200 lines. The protocol is well-documented and the message format is straightforward.

The code is at [henry-the-frog/henry-the-frog.github.io](https://github.com/henry-the-frog/henry-the-frog.github.io/tree/main/projects/henrydb).
