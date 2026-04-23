# Extraction Lessons & AST Mismatch Pattern (2026-04-22)

## The AST Format Mismatch Pattern

**Found 4 instances today in the Volcano planner:**

1. **CTE type**: Parser produces `{type: 'SELECT', ctes: [...]}` but Volcano checked `type === 'WITH'`
2. **Histogram data**: `{lo, hi, count, ndv}` bucket objects compared against scalar values
3. **HAVING aggregate arg**: Parser gives `{type: 'column_ref', name: 'val'}` for HAVING, but `'val'` (string) for SELECT columns
4. **Aggregate arg generally**: `SUM(a+b)` has object arg `{type: 'arith', ...}`, but `SUM(val)` has string arg `'val'`

**Root cause**: The SQL parser is inconsistent. Simple expressions produce string shortcuts (`'val'`), complex ones produce AST objects (`{type: 'column_ref', name: 'val'}`). Consumers must handle both.

**Mitigation**: Always normalize with `typeof x === 'object' ? x.name : x`

**TODO**: Audit all places in volcano-planner.js and select-inner.js that access parser fields.

## Extraction Lessons

### The `db` First-Param Pattern Works
All 7 extractions used the same pattern: `this._method(args)` → `function method(db, args)`. Delegation stubs in db.js: `_method(args) { return _methodImpl(this, args); }`. Clean, predictable, testable.

### Common Extraction Bugs
1. **Missing imports**: Extracted code references functions that were previously available through closure scope (e.g., `pushdownPredicates`)
2. **Bare `this`**: `sed 's/this\./db./g'` misses `new PlanBuilder(this)` → need to also check `\bthis\b` without `.`
3. **Wrong line ranges**: Adjacent methods can be grabbed accidentally. Always verify boundaries with grep.
4. **Method vs function**: `_method(args) {` needs conversion to `export function method(db, args) {`

### Size Estimates Were Often Wrong
- GROUP BY estimated 166 LOC, actual 525 LOC (3x off)
- _withCTEs estimated 350 LOC, actual 159 LOC (2x off)
- Join executor estimated 1200 LOC, actual 1074 LOC (close)

Always count before committing to an extraction.

### db.js Reduction Progress
- Peak: ~10,000 LOC
- After session: 3,546 LOC
- Reduction: **65%** (~6,454 LOC extracted)
- Files created: join-executor, group-by-executor, explain-executor, plan-format, select-inner, cte-executor

### Total Bugs Found: 7
1. tpch-compiled: threshold vs test scale mismatch
2. Histogram selectivity: object vs number comparison
3. CTE AST: type='SELECT' vs type='WITH'
4. HAVING aggregate arg: object vs string
5. Aggregate arg normalization (4 instances)
6. explain-executor: bare `this` references (4 instances)
7. UNION EXPLAIN ANALYZE: wrong dispatch method

### Correlated IN Subquery Bug Fix (2026-04-23)
**Bug:** `val IN (SELECT MAX(val) FROM t t2 WHERE t2.grp = t1.grp)` returned all rows instead of matching ones.
**Root cause:** Batch decorrelation removed the correlation predicate (`t2.grp = t1.grp`) but didn't add `GROUP BY grp` to the inner query. Without GROUP BY, `MAX(val)` aggregated the entire table. Also, column alias `__corr_t2_grp` wasn't appearing in GROUP BY query output because the engine's GROUP BY handler doesn't project additional SELECT columns.
**Fix:** Generate SQL string (not AST) for inner query with explicit GROUP BY and unqualified column names. Lookup correlation key by unqualified name in result rows.
**Lesson:** When removing correlation predicates from a subquery, check if the correlation was implicitly grouping rows. If the subquery has aggregates but no GROUP BY, the correlation predicate WAS the GROUP BY.

### Batch Decorrelation Operator Bug (2026-04-23)
**Bug:** WHERE clause builder in tryBatchDecorrelate hardcoded '=' for ALL predicates. So `sc.score < 80` became `sc.score = 80` in the generated SQL.
**Impact:** 3+ stress tests failing, data incorrectness in correlated IN with non-equality inner predicates.
**Fix:** Added serializeExpr() utility that properly renders COMPARE, AND, OR, literal, column_ref nodes to SQL. Also added JOIN clause support.
**Lesson:** When building SQL from AST, always preserve the original operator. Never assume '=' for arbitrary predicates.
