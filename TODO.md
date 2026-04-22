## TODO

### Urgent
(none)

### Normal
- HenryDB: Volcano engine WIRED into db.js! Hash join now used for equi-joins. Next: RIGHT/FULL join support in Volcano, INLJ LEFT join. (since 2026-04-21)
- HenryDB: db.js now at 4939 lines (SUB-5000 achieved!) — further extraction possible (join exec ~220, EXPLAIN ~350, GROUP BY ~166) (since 2026-04-20, updated 2026-04-21)
- HenryDB: persistent-db.test.js "joins work with persistent storage" has pre-existing bug — returns 6 rows instead of 3. (since 2026-04-21)
- HenryDB: explain-analyze.test.js "EXPLAIN ANALYZE with JOIN" has pre-existing failure. (since 2026-04-21)
- HenryDB: sql-compat.test.js "full-text search" has pre-existing failure. (since 2026-04-21)
- HenryDB: Volcano planner fully cost-based. Consider integrating table stats from ANALYZE. (since 2026-04-21, updated 2026-04-21)

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration

### Tomorrow Ideas (Session C)
- RISC-V liveness-based register allocation (algorithmic, different from db work)
- Neural-net: stress-test transformer attention implementation
- HenryDB: RIGHT/FULL join in Volcano, INLJ LEFT support
- Git: explore current state and find depth opportunities

### Quick Wins for Tomorrow
- SAT solver: SMT strict inequality bug — _processAssertion ignores < and >. 8-line fix: add else-if for '>' (→ >=, value+1) and '<' (→ <=, value-1). See smt.cjs line ~720.

### Blog Post Idea
- "Wiring a Volcano Engine into a Database" — from Feature Theater to 37x speedup. Covers the integration strategy, EvalPlanQual bug find, and benchmark results. Good technical post.
