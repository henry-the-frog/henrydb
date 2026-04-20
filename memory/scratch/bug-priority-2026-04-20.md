# HenryDB Bug Priority List (from Session B, Apr 20)

## P0 — CRITICAL
1. **Division truncation** (5 min) — `10.0/3=3`. Fix: tag `isFloat` in tokenizer. File: sql.js:83, db.js:5875.
2. **Hash join dead code** (30 min) — All joins NL. Fix: equi-join hash in `_executeJoin`. File: db.js:2430.
3. **Index after rollback** (60 min) — PK lookup returns empty after UPDATE+ROLLBACK. Fix: restore B-tree on rollback.
4. **CASE WHEN always true** (5 min) — `_evalExpr` default=true. Fix: change to `Boolean(this._evalValue(expr,row))`. File: db.js:5822.

## P1 — Significant
5. **SELECT boolean exprs** (120 min) — parseSelectColumn can't produce IS NULL, BETWEEN, IN, LIKE, comparisons. Fix: unify with parseExpr.
6. **SUM(empty)=0** (5 min) — Should return NULL. File: db.js (aggregate init).
7. **LIMIT 0** (5 min) — Returns all rows. File: db.js (LIMIT handling).
8. **Write skew** — SSI gap. May need investigation.

## P2 — Moderate
9. **Recursive CTE WHERE arithmetic** — Parser issue with `+` in WHERE of recursive member.
10. **MERGE subquery USING** — Parser limitation.
11. **CAST numeric identity** — `CAST(10 AS REAL)` → still integer.

## Fix Order (Tomorrow)
1. Quick wins: #1, #4, #6, #7 (20 min total)
2. Hash join: #2 (30 min)
3. Index rollback: #3 (60 min)
4. Parser unification: #5 (120 min — may span sessions)
5. Run full test suite + stress tests after each fix
