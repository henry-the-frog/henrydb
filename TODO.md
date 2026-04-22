## TODO

### Urgent
(none)

### Normal
- HenryDB: db.js now at 4939 lines (SUB-5000 achieved!) — further extraction possible (join exec ~220, EXPLAIN ~350, GROUP BY ~166) (since 2026-04-20, updated 2026-04-21)
- HenryDB: Volcano planner fully cost-based. Consider integrating table stats from ANALYZE. (since 2026-04-21, updated 2026-04-21)
- HenryDB: ORDER BY resolves to table column before SELECT alias — should prefer aliases. Found in FULL JOIN + COALESCE test. (since 2026-04-21)

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
