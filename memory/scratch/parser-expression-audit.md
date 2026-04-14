# Parser Expression Audit Pattern

uses: 1
created: 2026-04-13
tags: henrydb, parser, sql, bug-pattern

## The Bug

SQL parser used `parsePrimary()` (single token) instead of `parseExpression()` (full expression tree) in 7+ locations. This meant any compound expression was silently truncated:

- `WHERE id = 2 - 1` → parsed as `WHERE id = 2` (RHS expression dropped)
- `INSERT VALUES (5 * 20)` → parsed as `INSERT VALUES (5)`
- `BETWEEN 1 AND 2+3` → parsed as `BETWEEN 1 AND 2`
- `IN (1+1, 2*3)` → only parsed first token of each element

## Locations Found (Apr 13)

1. Comparison RHS (CRITICAL — `WHERE` clauses)
2. BETWEEN lower/upper bounds
3. INSERT VALUES list items
4. IN/NOT IN list items
5. UPDATE SET subqueries
6. Window PARTITION BY expressions
7. CREATE TABLE DEFAULT values
8. LIMIT/OFFSET expressions
9. RETURNING expressions

## The Fix

Upgraded all 7+ `parsePrimary()` calls to `parseExpression()` with appropriate precedence.

## Prevention

When adding a new SQL clause that accepts a value:
1. **Always use `parseExpression()`**, not `parsePrimary()`
2. `parsePrimary()` is ONLY for the leaf nodes of expressions (literals, column refs, function calls)
3. Test with compound expressions: `a + b`, `a * (b + c)`, subqueries
4. The differential fuzzer would catch many of these — run it after parser changes

## Meta-Lesson

This is a "copy-paste inheritance" bug. The first clause probably used `parsePrimary()` correctly (for a simple case), and subsequent clauses copied the pattern without considering expressions. **When copying parser code for a new clause, always ask: "Does this accept an expression or just a primary?"**
