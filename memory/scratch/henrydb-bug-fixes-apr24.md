# HenryDB Bug Fixes Learning (Apr 24, 2026)

## Critical Bugs Found & Fixed

### 1. Equi-join Key Swap (CRITICAL)
**Location:** `src/volcano-planner.js` → `extractEquiJoinKeys()`
**Bug:** When ON clause column order didn't match table order (e.g., `ON d.id = e.dept_id` where d is right table), buildKey and probeKey were swapped.
**Impact:** ALL qualified-name joins produced cross joins instead of inner joins.
**Fix:** Check which table each qualified column belongs to before assigning build/probe keys.
**Learning:** Join key extraction must be table-aware, not just position-aware.

### 2. Float Division Truncation
**Location:** `src/sql.js` (tokenizer) + `src/expression-evaluator.js`
**Bug:** `parseFloat("10.0") === 10` and `Number.isInteger(10) === true`, so `10.0 / 3 → 3` (integer division).
**Fix:** Propagate `isFloat` flag from tokenizer → parser → evaluator.
**Learning:** JS loses float literal type information. Must preserve it explicitly.

### 3. evalExpr Default: true
**Location:** `src/expression-evaluator.js` → `_evalExpr()` default case
**Bug:** Any unrecognized expression type returned `true`. This made `CASE WHEN NULL` always match the THEN branch.
**Fix:** Default case now evaluates the expression as a value and checks truthiness.
**Learning:** Default cases should be safe (false/null), not optimistic (true).

### 4. View-Table JOIN Skipped
**Location:** `src/db.js` → view resolution path
**Bug:** When FROM was a view, the handler returned early without processing any JOINs.
**Impact:** `SELECT * FROM view JOIN table ON ...` only returned view columns.
**Fix:** Add join processing before WHERE filtering in the view path.
**Learning:** Every early-return path in query execution needs the full pipeline (joins, where, order, limit).

### 5. NATURAL JOIN Not Implemented
**Bug:** `NATURAL` not in keywords list → tokenized as IDENT → parseJoin didn't see it.
**Fix:** Add to keywords, parseJoin, and auto-generate ON from common columns.

### 6. Trigger NEW/OLD Not Resolved
**Bug:** `_fireTriggers()` executed bodySql verbatim, never replacing NEW.col/OLD.col.
**Fix:** Text substitution of NEW.col → value before executing.
**Learning:** SQL text substitution is a quick approach for triggers. Proper AST rewriting would be better long-term.

### 7. CTE INSERT Not Handled
**Bug:** WITH...INSERT parsed incorrectly (parser only allowed WITH...SELECT).
**Fix:** parseWith() now checks for INSERT after CTE definitions. insertSelect() resolves CTEs before executing.

## Pattern: Many bugs are "path not handled"
5 of 7 bugs were cases where a code path simply didn't handle a feature:
- View path didn't handle joins
- WITH didn't handle INSERT
- NATURAL not in keywords
- Triggers didn't resolve NEW/OLD
- UNIQUE constraint not creating indexes

This suggests the main risk area is feature combinations that weren't tested together.
