# Failures & Patterns

## 2026-04-09 HenryDB Depth Day

### Critical Bugs Found and Fixed
1. **ALL SELECT queries through server were broken** — `QueryCache.extractTables` was missing, and `set()` args were swapped. Every SELECT threw an error.
2. **O(n²) WAL flush** — `_flushToStable()` used `Array.includes()` (O(n) per record). Made INSERT 8x slower at scale. Fixed with index tracking.
3. **7 test files had ghost imports** — WAL_TYPES, WALRecord, recoverFromWAL, recoverToTimestamp didn't exist. Tests were never running.
4. **LSM tree API mismatches** — `insert()` vs `set()`, `find()` vs `get()`, `!== null` vs `!== undefined`. LSM was completely non-functional.
5. **Cuckoo hash operator precedence** — `>>> 0 % capacity` parsed as `>>> (0 % capacity)`. Hash function always returned raw hash, never modulo capacity.
6. **PlanCache LRU used Date.now()** — sub-millisecond operations all got same timestamp, making LRU random. Fixed with monotonic counter.
7. **DeadlockDetector API drift** — `registerTxn` vs `registerTransaction`, `addWait` vs `recordWait`.
8. **BufferPool.fetchPage ignores disk callback** — second arg (diskReadFn) silently ignored. All file-backed pages loaded as zeros.
9. **BufferPool.flushAll ignores evict callback** — always wrote to in-memory Map, never disk. Dirty pages never persisted.
10. **BufferPool._readFromDisk returns zeros for unknowns** — silently returns zeroed buffer instead of throwing, masking real I/O bugs.
11. **EXPLAIN text format returned type 'ROWS' not 'PLAN'** — broke all 15+ EXPLAIN tests. Plan array was present but not included in response.

### Pattern: Layer Interface Mismatches
BufferPool was designed for in-memory simulation. When FileBackedHeap passed callbacks, they were silently ignored because the method signatures didn't accept them. **JavaScript doesn't warn about extra args.** This class of bug is invisible in unit tests — only exposed by integration tests that cross layer boundaries.

### Lesson: Test the Reopen Path
3000+ tests passed but ZERO tested the close → reopen → read-back cycle with file-backed pages. The entire persistence layer was non-functional. **Always test the full lifecycle, not just individual operations.**

## 2026-04-09 (Session B evening)
- **QueryCache.extractTables REGRESSION** — Same exact bug fixed in Session A (T11) was broken again in Session B. Root cause: `QueryCache.extractTables` is a static method that was never actually added to the class, and `cache.set()` was called with wrong argument order. This means ALL SELECT queries through pg wire protocol silently errored. The bug survived because:
  1. Session A's "fix" in T11 was lost (possibly a different code path or the fix wasn't complete)
  2. The 14 server tests weren't run as part of the broader test suite sweeps (T43, T83)
  3. In-memory `db.execute()` works fine — the bug only manifests through the server layer
- **Lesson: Always run server tests after touching QueryCache or server.js.** The pg wire protocol is the user-facing API and it was completely broken for reads.
- **REGRESSION GUARD:** This bug has appeared TWICE in one day (Sessions A and B). Any commit that touches `server.cjs`, `db.js`, or `query-cache.js` MUST include a server test run. Consider adding a test that explicitly verifies `QueryCache.extractTables` exists as a static method.

## 2026-04-08
- **Dashboard API routes 404** — Server runs on port 3000, responds to requests, but archive-day and regenerate endpoints return {"error":"Not found"}. Server was rebuilt from scratch this morning — likely route naming mismatch between generate.cjs expectations and new server.js routes.
- **Knowledge system underutilized** — 468 BUILD tasks today but only 1 reference to lessons/failures in daily log. THINK/PLAN tasks didn't consult failures.md. Pattern: high-velocity build sessions skip knowledge feedback loops.

## 2026-04-07
- **Dashboard server down** — port 3000 unreachable during both MAINTAIN tasks (T4 and evening review). Archive-day and regenerate both failed. Cause unknown — server may not have been restarted after last reboot. This is 2nd occurrence (also failed during Session C part 2 MAINTAIN). Pattern: dashboard server doesn't auto-start.

## JS Null Coercion in Database Comparisons (2026-04-10)
**Pattern:** Using `>=`, `<=`, `>`, `<` with null values in JavaScript
**What happened:** `null >= -10` evaluates to `true` because JS coerces null to 0 for numeric comparisons. This caused:
1. BETWEEN with NULL: `null BETWEEN -10 AND 15` returned true (null → 0, 0 is between -10 and 15)
2. ORDER BY with NULL: null compared as 0, sorting it among real values instead of first/last

**Root cause:** JavaScript's abstract relational comparison coerces null to 0.
- `null > 5` → `false` (0 > 5)
- `null >= -10` → `true` (0 >= -10)  
- `null == 0` → `false` (== uses different rules than >/<)

**Fix:** Always check for null/undefined BEFORE any comparison:
```javascript
if (val === null || val === undefined) return false; // for BETWEEN
if (av == null) return -1; // for ORDER BY (null is smallest)
```

**Prevention:** When writing comparison logic for database values:
1. ALWAYS add null guards before `>`, `<`, `>=`, `<=`
2. Use `=== null` checks, not `== null` (catches undefined too)
3. SQL standard: any comparison with NULL returns NULL (false)
4. Use differential fuzzer against SQLite to catch these

## Query Cache Wrapper Bug (2026-04-10)
**Pattern:** Cache returning wrapper object instead of cached value
**What happened:** `QueryCache.get()` returned `{result, timestamp}` but caller expected just `result`. 
**Fix:** Access `cached.result` instead of `cached` directly.
**Prevention:** When adding caching, test the cache hit path separately from cache miss.

## Query Cache Stale After ROLLBACK (2026-04-10 Session C)
**Pattern:** Query cache not invalidated on transaction state changes
**What happened:** After BEGIN → UPDATE → SELECT (cached) → ROLLBACK, the next SELECT returned the cached (rolled-back) value instead of the original.
**Root cause:** Server's QueryCache was invalidated on DML (UPDATE invalidates "accounts") but NOT on ROLLBACK or COMMIT. The SELECT during the transaction cached {balance: 50}, then ROLLBACK properly undid the MVCC change but the cache still held the stale result.
**Fix:** Call `this._queryCache.invalidateAll()` on both COMMIT and ROLLBACK in `_interceptSystemQuery`.
**Key insight:** The MVCC engine, heap storage, and scan interceptors were all correct. The bug was in a completely different subsystem (query cache). When debugging multi-layer systems, check caches early.
**Prevention:** Any transaction state change (COMMIT, ROLLBACK, ROLLBACK TO SAVEPOINT) must invalidate the query cache.

## 2026-04-11: Persistence Recovery Data Loss Bugs

**Bug pattern: State transitions at boundaries**
- BufferPool had no invalidateAll() — recovery cleared disk pages but cache served stale data
- heap._rowCount not reset before recovery replay → double-counting
- recoverFromFileWAL wiped ALL pages even after checkpoint+truncate → DATA LOSS (50 rows destroyed to replay 1)
- lastAppliedLSN was in-memory only, never persisted → recovery couldn't distinguish "needs replay" from "already applied"  
- close() didn't update lastAppliedLSN after flush → next reopen replayed already-flushed records

**Root insight:** Each component worked correctly in isolation. Bugs lived in handoffs — where one subsystem assumed another's state. Integration testing > unit testing for databases.
**Prevention:** ARIES recovery requires persistent LSN tracking. Always test the scary scenarios: tiny buffer pools, crash without close(), checkpoint+truncate+reopen.

## 2026-04-11: Query Cache + Adaptive Engine Bypass MVCC

**Bug:** Wire protocol server's query cache and adaptive engine served results that bypassed MVCC:
- Query cache returned stale cached results inside BEGIN...COMMIT blocks (ignoring uncommitted changes)
- Adaptive engine executed SELECTs without transaction context (no session.execute routing)
- Together, these made UPDATE invisible to subsequent SELECT within the same transaction

**Fix:** Skip cache and adaptive engine when `conn.txStatus === 'T'` (in-transaction)

**Related:** Adaptive engine too broadly eligible — accepted DISTINCT, OFFSET, NOT IN queries but didn't implement them. Fix: exclusion checks in `_isAdaptiveEligible()`

**Prevention:** Any query shortcut (cache, adaptive engine, query rewriter) must check transaction state. If in a transaction, MUST route through the session's MVCC layer.
