# Failures & Patterns

## 2026-04-13: DDL Persistence Pattern — The "Close/Reopen" Bug Family

**Summary:** 5 separate bugs (#14-16 + 2 variants) from the same root cause: TransactionalDatabase's catalog only tracked CREATE TABLE and CREATE INDEX. Every other DDL statement (CREATE VIEW, CREATE TRIGGER, CREATE MATERIALIZED VIEW, ALTER TABLE) was lost on close/reopen.

**Root cause:** The DDL detection used `trimmed.startsWith('CREATE TABLE') || trimmed.startsWith('CREATE INDEX')` — a manually-maintained allowlist that missed new DDL types. Also, ALTER TABLE wasn't tracked at all (it modifies existing objects rather than creating new ones).

**Fix:** Changed to `trimmed.startsWith('CREATE ')` to catch ALL CREATE statements. Added `_trackAlter()` that reconstructs the CREATE TABLE SQL from the current schema after ALTER TABLE.

**Pattern:** Allowlists rot. When you add a new DDL type to the SQL parser, you must also add it to the persistence layer. A negative approach ("catch all DDL except...") is more resilient than a positive approach ("catch TABLE, INDEX, VIEW, ..."). The fix catches this class of bugs permanently.

**Related bugs in this session:**
- Bug #1: Frozen txId 0 not visible after recovery (recovery path ≠ operational path)
- Bug #2: Indexes empty after reopen (derived state not rebuilt)
- Bug #3: PK not enforced (index check never wired for basic INSERT)
- Bug #7: Savepoint-revoked rows persist (xmax=-2 not physicalized)
- Bug #14: Views don't persist
- Bug #15: Triggers don't persist
- Bug #16: ALTER TABLE column additions don't persist

## 2026-04-13: SSI snapshot.has Called on Plain Object

**Bug:** SSI `detectConflicts` at line 75 called `snapshot.has(txId)` but `snapshot` is `{ xmin, xmax, activeSet: Set }` — a plain object, not a Set.
**Root cause:** The snapshot was changed from a Set to a structured object at some point, but ssi.js wasn't updated.
**Fix:** Changed to check `snapshot.activeSet.has(txId) || txId >= snapshot.xmax` for proper concurrent-writer detection.
**Pattern:** API evolution creates stale call sites. When an internal data structure's shape changes, all consumers need updating. Type checking would catch this (TypeScript, or at least JSDoc).

## 2026-04-13: Index Empty After Reopen + PK Not Enforced

**Bug 1:** After close/reopen, indexes were empty. `WHERE id = 25` returned 0 rows even though full scan found all data.
**Root cause:** Catalog replay creates `CREATE TABLE` + `CREATE INDEX` *before* WAL recovery fills the heap. So indexes were created but never populated with recovered data.
**Fix:** After WAL recovery, scan heap and insert all rows into all indexes.

**Bug 2:** PK constraint not enforced during INSERT after reopen. Duplicate PKs accepted silently.
**Root cause:** `BTree.insert()` doesn't check uniqueness. The PK scan check in `_executeInsert` only runs when `ast.onConflict` is present. Normal INSERTs relied on... nothing.
**Fix:** Added explicit uniqueness check via `index.range(key, key)` before inserting into unique indexes.

**Pattern:** Recovery creates "fresh" derived objects (indexes, caches) but doesn't populate them from restored primary data. Every derived structure needs explicit rebuild after recovery. Also: PK enforcement was never properly wired for basic INSERTs — it only worked "by accident" because in-memory tables always had the index populated at insert time.

## 2026-04-13: Frozen TxId 0 Not Visible After Recovery

**Bug:** After close/reopen, TransactionalDatabase returned 0 rows even though data was correctly persisted on disk.
**Root cause:** During WAL recovery, rebuilt version maps set xmin=0 for all recovered rows. But MVCCManager.isVisible() checked if txId 0 was in committedTxns — it wasn't (it's a synthetic ID). So all recovered rows were invisible.
**Fix:** Treat txId 0 as "frozen" — always visible, like PostgreSQL's frozen tuple optimization.
**Pattern:** Recovery paths create synthetic state that doesn't match the normal operational path. Any MVCC visibility check needs special cases for recovered/pre-existing data.
**Related:** MVCCManager.commit()/rollback() accepted tx objects but TransactionalDatabase passed txId numbers. SSIManager accepted txId numbers. Made both accept either.

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

## 2026-04-15: LATERAL JOIN WHERE Filter Dropped

**Bug:** WHERE predicates on LATERAL subquery columns were silently ignored.
**Root cause:** pushdownPredicates() pushed WHERE conditions referencing the lateral alias (e.g., sub.max_sal IS NOT NULL) into join.filter. But _executeJoin()'s LATERAL path returned results without checking join.filter. The predicates were pushed but never applied.
**Fix:** Apply join.filter to the LATERAL result rows before returning.
**Key insight:** When adding a query optimization pass (like predicate pushdown), you must verify that ALL join execution paths honor the filter placement. The LATERAL path was added later and missed this.
**Prevention:** Any new join type must check for join.filter. The pushdown optimizer doesn't know about execution paths — it just assigns filters.

## 2026-04-15: Window Function Frame Spec Bugs

**RANGE BETWEEN Bug:** getFrameBounds() treated all OFFSET frame specs as ROWS offsets (row-position based). RANGE offsets should be value-based (current ORDER BY value ± offset).
**FIRST_VALUE/LAST_VALUE Bug:** Both ignored the frame spec entirely. FIRST_VALUE always used partition[0], LAST_VALUE always used current row. Should use getFrameBounds() start/end indices.
**Key insight:** When adding new functionality to getFrameBounds (like RANGE mode), every function that calls it needs to actually use the bounds — FIRST_VALUE/LAST_VALUE were hardcoded to bypass it.

## 2026-04-18: Feature Capability Detection → Data Loss

### HOT Chain + FileBackedHeap (CRITICAL)
**Bug:** `isHotUpdate = true` but `table.heap.addHotChain` didn't exist (FileBackedHeap). Code entered HOT path, silently failed to create chain, AND skipped index updates. Rows became invisible to all index-based lookups.

**Root cause:** Guard clause `if (table.heap.addHotChain) { ... }` inside the `if (isHotUpdate)` branch. When the condition was false, execution fell out of the HOT block without entering the `else` (non-HOT) block that updates indexes.

**Fix:** Changed to `if (isHotUpdate && table.heap.addHotChain)` — the capability check gates the entire HOT path, not just the chain creation.

**Pattern:** Feature detection that only guards the optimization but not the fallback = data loss.

### WAL Recovery INSERT Bypassing Indexes (CRITICAL)  
**Bug:** `recoverFromWAL()` used `tableObj.heap.insert(row)` which bypasses B-tree index maintenance. After recovery, PK lookups returned empty despite rows existing in heap.

**Root cause:** The recovery code checked `if (tableObj.heap)` first (always true), making the `else if (db.execute)` SQL path dead code.

**Fix:** Reversed priority: prefer `db.execute('INSERT...')` when available, fall back to heap.insert only for raw heap objects.

**Pattern:** "Always true" conditions before more specific conditions make later branches dead code. Order matters.

## 2026-04-17: Four more backward pass bugs (gradient verification round 2)
- **Root cause pattern**: Complex forward passes (routing, ODE solving, capsule routing) have the highest backward bug rate
- **KAN**: Clamping in forward without matching boundary handling in backward
- **MoE**: Weight-sharing cache invalidation — when same module processes multiple inputs, last call overwrites caches
- **CapsuleLayer**: Side effects in backward (inline weight update) — violates functional contract
- **NeuralODE**: Adjoint method not implemented correctly — variable never updated in loop
- **Lesson**: Any module with comments like "simplified" or "approximate" in backward is suspicious

## 2026-04-17: MVCC Snapshot Isolation Completely Broken + SMT String Assertion
### HenryDB MVCC (CRITICAL x3)
1. **BEGIN never set txId**: WAL auto-committed uncommitted DML operations
2. **Repeatable Read violated**: read() used simple txId comparison instead of snapshot visibility
3. **No write-write conflict detection**: Lost updates possible with concurrent transactions
4. **GROUP BY + window function**: Window columns silently dropped
- **Root cause pattern**: Each subsystem implemented its contract partially:
  - WAL had txId tracking but BEGIN didn't provide txIds
  - MVCC had snapshots but read() didn't use them
  - write() had no conflict check at all
  - GROUP BY and window functions were independent code paths

### SAT Solver SMT
- **String assertions silently ignored**: _processAssertion() checked Array.isArray() which is false for strings
- **Root cause**: Parser and processor used different types (string vs array)

### Meta-lesson
Three bugs were "the feature exists but isn't wired up":
1. WAL txId tracking exists but BEGIN doesn't set txId
2. Snapshot exists but read() doesn't use it
3. Parser exists but processor doesn't call it

This is a specific failure mode of incremental development: you build the pieces, test them individually, but forget to connect them. The fix is **contract testing at integration boundaries** — which is exactly what found all of these bugs.
