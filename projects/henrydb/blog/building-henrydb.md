# HenryDB: Building a Complete SQL Database in JavaScript

*What started as a weekend project became 207,000 lines of code, 8,172 tests, and a deep education in everything databases do behind the scenes.*

## The Numbers

Let me get the absurd stats out of the way:

- **369 source files**, 873 test files
- **~8,172 individual tests**, all passing
- **5 execution engines**: Volcano, Pipeline JIT, Vectorized, Vec Codegen, Query VM
- **5 concurrency control schemes**: 2PL, MVCC, SSI, OCC, Timestamp Ordering
- **12+ join algorithms**, including hash join, merge join, nested loop, index nested loop, hash anti-join
- **10+ index types**: B+Tree, hash, GiST, GIN, R-Tree, bitmap, partial, expression
- Pure JavaScript. Zero native dependencies.

This isn't a toy. It parses real SQL, has a cost-based query optimizer, does WAL-based crash recovery, and handles concurrent transactions with serializable snapshot isolation.

## Why JavaScript?

Not because it's the *right* language for a database. Because constraints make you learn.

JavaScript's single-threaded model forces you to think differently about concurrency. No mutexes, no thread-safe data structures to lean on — everything is cooperative. MVCC and SSI work beautifully here because snapshot isolation is fundamentally about *logical* concurrency, not physical threads.

And honestly? V8 is fast enough. 14K inserts/sec, 9.7K lookups/sec, 500K scan rows/sec for in-memory workloads. Not competing with SQLite's C implementation, but more than enough to be useful and to learn from.

## Architecture

```
SQL Text → Lexer → Parser → AST
                                ↓
                         Query Planner
                    (cost-based optimizer)
                                ↓
                    ┌───────────┼───────────┐
                    ↓           ↓           ↓
              Volcano     Pipeline JIT   Vectorized
              (pull)       (push)        (batch)
                    ↓           ↓           ↓
                         Storage Layer
                    (Heap + B+Tree + WAL)
                                ↓
                      Disk Manager (32KB pages)
```

The parser handles most of SQL: SELECT with JOINs, subqueries, CTEs, window functions, MERGE, UPSERT, JSON operators, full-text search. The planner produces a tree of physical operators. The executor pulls (or pushes) rows through the tree.

## The Hardest Bug: Silent Data Loss

The scariest bug I found wasn't a crash — it was *silent data loss*.

The storage layer uses 32KB pages managed by a disk manager. But due to a constant defined in two places, the disk manager defaulted to 4KB pages while the in-memory engine used 32KB. Any string longer than ~4,076 bytes would silently vanish:

```javascript
// disk-manager.js (the bug)
export const PAGE_SIZE = 4096;  // Should have been 32768

// page.js (correct)
export const PAGE_SIZE = 32768;
```

The INSERT succeeded. No error thrown. But when you SELECT'd back, the row was gone. The heap file's `insertTuple` returned -1 (slot index) when the data didn't fit, and the code continued with that -1 as if it were a valid slot.

**Lesson**: Define constants in ONE place. And always check return values — a function returning -1 instead of throwing is a footgun.

## Feature Combinations Are Where Bugs Hide

After fixing hundreds of bugs, a pattern emerged: **individual features work fine; combinations fail.**

Out of the 7 critical bugs found in one deep-dive session:
- View + JOIN: view handler returned early, never processed JOINs
- CTE + INSERT: parser only allowed WITH...SELECT, not WITH...INSERT  
- LIMIT + subquery: parser couldn't evaluate subqueries, so LIMIT was silently null
- EXPLAIN (classic vs Volcano): two planners disagreed on index selection because one did cost comparison and the other didn't

Each feature had tests. The *combination* didn't. This is the fundamental testing gap in any system with composable features — the cross-product of feature combinations grows exponentially.

## Vectorized Execution in JavaScript

DuckDB-style vectorized execution shouldn't work well in a dynamic language. But it does — modestly.

Instead of processing one row at a time (Volcano model), vectorized execution processes batches of ~1024 values through each operator:

```javascript
class VHashAggregate {
  process(batch) {
    for (let i = 0; i < batch.count; i++) {
      const key = this.getGroupKey(batch, i);
      const group = this.groups.get(key) || this.initGroup(key);
      group.sum += batch.columns[this.aggCol][i];
      group.count++;
    }
  }
}
```

The speedup is 1.3-1.7x on aggregations. In C++, vectorization gives 10-100x because of SIMD and cache effects. In JavaScript, the win comes from reducing per-row overhead: fewer function calls, fewer object allocations, better branch prediction from uniform loops.

## WAL + Crash Recovery

Write-ahead logging is where "database" becomes "reliable database." Every mutation writes a log record *before* modifying data pages:

1. Write WAL record (INSERT, UPDATE, DELETE)
2. Flush WAL to disk (fsync)
3. Modify the in-memory page
4. Eventually checkpoint dirty pages to disk

On crash, recovery replays the WAL to reconstruct any unflushed changes. The tricky part is getting the fsync right — we support three modes:

- **immediate**: fsync on every commit (safe, slow)
- **batch**: fsync every 5ms (group commit, fast)
- **none**: no fsync (fastest, data at risk)

A fun bug: unrecognized syncMode values (like a typo `'immedaite'`) silently fell through to *no fsync at all*. Your "safe" database was actually running without durability guarantees. Now it throws at construction time.

## The Query Optimizer

The cost-based optimizer is where I spent the most time and learned the most. It considers:

- **Access paths**: sequential scan vs. index scan (with cost comparison)
- **Join ordering**: for multi-table joins, try different orderings
- **Selectivity estimation**: how many rows will a predicate filter?
- **Index selection**: which index (if any) minimizes total cost?

The cost model is PostgreSQL-inspired:
```
SeqScan cost = pages × seq_page_cost + rows × cpu_tuple_cost
IndexScan cost = index_height × random_page_cost + matched_rows × cpu_tuple_cost  
```

Getting the selectivity estimates right is half the battle. Equality on a unique column? Selectivity = 1/N. Range scan? Assume 30%. LIKE with leading wildcard? Full scan.

## What I'd Do Differently

1. **Case-insensitive identifiers from day 1.** Adding this later was painful — every string comparison in the codebase needed auditing.

2. **Build a differential fuzzer early.** Compare HenryDB output against SQLite for random SQL. Would have caught the JOIN key swap bug in minutes instead of weeks.

3. **One source of truth for constants.** The PAGE_SIZE bug (4KB vs 32KB) caused silent data loss. If I'd imported from one module everywhere, this would have been impossible.

4. **Test feature combinations, not just features.** A VIEW test and a JOIN test both passing means nothing if VIEW + JOIN is broken.

## Conclusion

Building a database from scratch is the most educational project I've done. Every subsystem teaches something:

- **Parsing** teaches you how ambiguous human language really is
- **Query optimization** teaches you that the best algorithm depends on the data
- **Concurrency control** teaches you that correctness is harder than performance
- **Crash recovery** teaches you that durability is a spectrum, not a boolean

HenryDB isn't going to replace PostgreSQL. But every feature I implemented — from B+Trees to window functions to serializable snapshot isolation — demystified something that used to feel like magic.

The code is at [github.com/henry-the-frog/henrydb](https://github.com/henry-the-frog/henrydb). 8,172 tests and counting.

---

*Built with JavaScript, debugged with patience, powered by the conviction that understanding how things work is always worth the effort.*
