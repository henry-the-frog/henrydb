# HenryDB Bug Priority List (Session B, Apr 20 — UPDATED)

## P0 — CRITICAL (correctness + performance)
1. **Division truncation** (5 min) — `10.0/3=3`. Fix: tag `isFloat` in tokenizer. File: sql.js:83, db.js:5875.
2. **Hash join dead code** (30 min) — All joins NL. Fix: equi-join hash in `_executeJoin`. File: db.js:2430.
3. **Index after rollback** (60 min) — PK lookup returns empty after UPDATE+ROLLBACK. Fix: restore B-tree on rollback.
4. **CASE WHEN always true** (5 min) — `_evalExpr` default=true. Fix: evaluate truthiness. File: db.js:5822.

## P1 — Significant (common patterns broken)
5. **SELECT boolean exprs** (120 min) — parseSelectColumn can't produce IS NULL, BETWEEN, IN, LIKE, comparisons. Fix: unify with parseExpr.
6. **SUM(empty)=0** (5 min) — Should return NULL. 
7. **LIMIT 0** (5 min) — Returns all rows.
8. **EXCLUDED in UPSERT** — ON CONFLICT DO UPDATE SET val = EXCLUDED.val → NULL. Context not passed.
9. **Trigger NEW/OLD** — Fire correctly but row references return NULL.
10. **NATURAL JOIN** — Acts as cross join, doesn't match on common columns.
11. **FULL OUTER JOIN** — Drops unmatched rows from right side.
12. **VIEW-TABLE JOIN** — Drops table-side columns from result.
13. **INSERT FROM CTE** — Silent no-op.
14. **UNIQUE constraint** — Table-level UNIQUE parses but doesn't enforce. Inline UNIQUE fails to parse.

## P2 — Moderate (edge cases, parser gaps)
15. **Recursive CTE WHERE arithmetic** — Parser error.
16. **MERGE subquery USING** — Parser limitation.
17. **CAST numeric identity** — `CAST(10 AS REAL)` returns integer.
18. **EXPLAIN ANALYZE actual_rows** — Shows scanned, not matched.
19. **Vectorized HashJoin** — buildColumns not iterable.
20. **CTE in subquery** — Parser error.

## REMOVED (False Positives)
- ~~FK CASCADE~~ — Works correctly (earlier test had wrong syntax)
- ~~FK SET NULL~~ — Works correctly
- ~~Write skew/SSI~~ — Intentional SI (not strict SSI)

## Fix Order (Tomorrow)
1. Quick wins: #1, #4, #6, #7 (20 min total)
2. Hash join: #2 (30 min)
3. Index rollback: #3 (60 min)
4. Parser unification: #5 (120+ min — enables fixing #8, #9, #10, etc.)
5. Regression test: run regression-2026-04-20.test.js after each fix
