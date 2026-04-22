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

## 2026-04-20: Method Naming Mismatch Class (3 instances)

### _tryVectorizedExecution → _selectInnerCore (CRITICAL)
**Bug:** `_tryVectorizedExecution` was called in `_select()` but the method was actually named `_selectInnerCore`. ALL SELECT queries through TransactionalDatabase crashed.
**Root cause:** Method was renamed during refactoring but the call site wasn't updated.
**Why undetected:** The method was only called for tables with 500+ rows via auto-vectorization, and most tests use small tables with Database (not TransactionalDatabase).

### _executeAst → execute_ast (5 call sites)
**Bug:** Private method name `_executeAst` was used in 5 places but the real method is `execute_ast`. Broke prepared statements, cursors, and MERGE with subqueries.
**Why undetected:** All affected features were new (added the same day).

### DEALLOCATE ALL → name parsing
**Bug:** `DEALLOCATE ALL` was parsed as `DEALLOCATE` with name `ALL` instead of the special keyword.
**Fix:** Check for `ast.name.toUpperCase() === 'ALL'` in addition to `ast.all`.

**Pattern:** JavaScript doesn't catch `this.undefinedMethod()` at parse time — only at runtime. Method renames are silent failures. 
**Prevention:** After renaming ANY method in db.js, run `grep -n "methodName" src/db.js` to find all call sites. Consider adding a constructor check that validates all expected methods exist.

## 2026-04-20: Tokenizer Duplicate Negative Number Check

**Bug:** `10-4` was tokenized as `NUMBER:10 NUMBER:-4` instead of `NUMBER:10 MINUS NUMBER:4`. Caused `ARRAY[10-4]` to fail.
**Root cause:** Two separate negative number checks in the tokenizer — one with a context guard (after comma, paren, operators), one without (raw `-` before digit). The unguarded one fired first.
**Fix:** Removed the unguarded check, added unary minus support in parsePrimary().
**Pattern:** Two code paths for the same thing always creates bugs. The fix improved both paths.

## 2026-04-20: Correlated Subquery Outer Scope Resolution

**Bug:** `EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey)` — the `o_orderkey` outer reference wasn't detected as correlated. The subquery was evaluated as uncorrelated, returning empty.
**Root cause:** `isCorrelated()` in decorrelate.js only compared column references against outer TABLE names/aliases. It never checked inner table column schemas. So unqualified column names that happened to not match any table name were treated as uncorrelated.
**Fix:** Added inner table column schema checking. If a referenced column doesn't exist in ANY inner table's schema, it must be an outer reference.
**Pattern:** Decorrelation must understand column namespaces, not just table names.

## 2026-04-20 Session B: Stress Test Findings

### Division Always Truncates (CRITICAL)
**Bug:** `SELECT 10.0 / 3` → 3 instead of 3.33. All floating-point division returns integer.
**Root cause:** sql.js tokenizer: `parseFloat("10.0")` returns JS `10`, then `Number.isInteger(10) === true`, so division does `Math.trunc()`.
**Fix needed:** Tag NUMBER tokens with `isFloat: true` when source contains decimal point. Use that tag in division instead of `Number.isInteger()`.
**Impact:** Every revenue/price calculation in every query is wrong. The TPC-H `l_extendedprice * (1 - l_discount)` always returns 0 because `1 - 0.05 = 0` after truncation.

### Hash Join Dead Code (CRITICAL)
**Bug:** planner.js has complete hash join and merge join implementations. db.js executor always uses nested loop. The planner's output is never consumed.
**Root cause:** `_executeJoinWithRows` in db.js doesn't check the planner. EXPLAIN shows NESTED_LOOP_JOIN regardless.
**Impact:** Multi-table joins 100-1000x slower than they should be. 500×2000 join: 17s instead of ~100ms.
**Fix needed:** Add equi-join detection + hash map build in `_executeJoinWithRows` (~30 lines).

### NULL IS NULL in SELECT (SIGNIFICANT)
**Bug:** `SELECT NULL IS NULL` → returns `{"NULL":"NULL"}` instead of `true`/`1`.
**Root cause:** `parseSelectColumn()` has separate expression parsing from `parseExpr()`. NULL keyword falls through to generic column-name handling. IS operator not checked in SELECT context.
**Pattern:** Dual expression parsing paths. Same root cause as `val IS NULL as is_null` returning column headers as values.

### Index Lookup After Rollback Returns Empty (CRITICAL)
**Bug:** After UPDATE+ROLLBACK in explicit transaction, `WHERE id = 3` (PK index lookup) returns empty, but full table scan and `WHERE id > 2` (range scan) both find the row.
**Repro:** Create table, insert rows, begin tx, UPDATE, rollback. Index equality lookup fails.
**Root cause:** UPDATE modifies B-tree index to point to new version. ROLLBACK restores heap data but doesn't restore the B-tree index entry to point to the original version.
**Impact:** Any index lookup after a rolled-back UPDATE on indexed columns silently returns wrong results. Data appears missing.
**Pattern:** Same class as MVCC index bypass — index maintenance and MVCC version visibility are not coordinated.

## 2026-04-22: Volcano Engine — AST Format Mismatch Bugs

### The Systemic Pattern
21 bugs found in one session, mostly from the same root cause: **parser AST format varies by context**, and the Volcano planner assumed a single format.

**Key examples:**
- **Bug #19 (CRITICAL):** Parser uses `'='` symbol for JOIN ON conditions but `'EQ'` for WHERE conditions. ALL JOIN ON equi-conditions fell through to cross-product NestedLoopJoin. Fix: add symbol operators to comparators and findEquiJoin.
- **Bug #20:** `extractEquiJoinKeys` returned buildKey/probeKey based on AST order without checking which qualified name (e.g., `o.product_id` vs `p.id`) belonged to which table. Cross-product for all qualified joins. Fix: match qualified prefix to table alias.
- **Bug #21:** `SUM(expr)` evaluated to 0. HashAggregate used `row[agg.column]` but for expression args, `agg.column` was a JSON-stringified AST node. Fix: pass valueGetter function for expression-type args.
- **Bug pattern:** LIKE (field vs expr), BETWEEN (left vs expr), IS NULL (left fallback), CASE (elseResult vs else), IN_SUBQUERY (lazy vs eager), arith (type=arith vs binary_expr), function (type=function vs function_call)

**Prevention:** Any new predicate/expression type must be tested with the full 41-query stress test before merge. The `volcano-correctness.test.js` (36 tests) and stress-test script catch these classes of bugs.
