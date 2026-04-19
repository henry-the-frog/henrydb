# Escaped Quotes Bug + Serialization Lessons

## Bug 1: Parser escaped quotes dead code
- `while (i < src.length && src[i] !== "'")` exits at first `'`
- The escaped quote check `if (src[i] === "'" && src[i + 1] === "'")` inside the loop was unreachable
- Fix: remove `!== "'"` from while condition, use explicit `break`
- **Pattern**: Short-circuit conditions in while loops can make inner escape logic dead code

## Bug 2: View serialization mismatch
- `toJSON()` stores views as AST objects (what the db uses internally)
- `fromJSON()` tried to reconstruct via SQL: `CREATE VIEW name AS ${viewData.sql}`
- But `viewData.sql` is undefined — views store `.query` (an AST)
- Fix: detect AST format and restore directly via `db.views.set()`
- **Pattern**: Serialization/deserialization asymmetry — toJSON stores internal format, fromJSON expects external format

## Bug 3: Identifier function calls not parsed in SELECT
- `parseSelectColumn()` only recognized KEYWORD tokens as functions (via SCALAR_FUNCTIONS set)
- IDENT tokens like `pg_stat_statements_reset` were treated as column references
- Fix: add IDENT + `(` detection before column ref fallback
- **Pattern**: Parser paths that handle keywords may not handle identifiers for the same construct

## Lesson
Breadth sprints leave bugs at the seams: escape handling, format mismatches, parser paths. Stress tests that exercise roundtrips (serialize→deserialize, SQL→parse→execute→SQL) are high-ROI.

uses: 1
created: 2026-04-18
