# parseSelectColumn vs parseExpr Gap Analysis

## What parseSelectColumn handles (works in SELECT)
- Number literals ✅
- String literals ✅  
- Column references ✅
- Table.column references ✅
- Arithmetic (+, -, *, /, %) ✅
- Parenthesized expressions ✅ (but only arithmetic inside)
- Function calls (UPPER, LENGTH, etc.) ✅
- Aggregate functions (COUNT, SUM, etc.) ✅
- Window functions (RANK, ROW_NUMBER, etc.) ✅
- CASE WHEN ... THEN ... END ✅
- CAST(expr AS type) ✅
- Scalar subqueries ((SELECT ...)) ✅
- COALESCE, NULLIF ✅
- EXTRACT, DATE_TRUNC ✅
- ARRAY[...] constructor ✅
- String concatenation (||) ✅

## What only parseExpr handles (BROKEN in SELECT)
- NULL literal → treated as column name "NULL"
- TRUE/FALSE literals → treated as column names
- IS NULL / IS NOT NULL → returns column headers as values
- BETWEEN x AND y → returns column name as value
- IN (list) → returns column name as value
- LIKE / ILIKE → returns left operand value
- EXISTS(...) → returns keyword "EXISTS" as column name
- Comparisons (>, <, =, <>) → returns left operand
- Boolean operators (AND, OR) → returns first value
- NOT → returns keyword "NOT" as column name

## Root Cause
`parseSelectColumn` is a ~300 line function with hand-coded special cases. When it reaches the end without matching any special case, it does `const col = advance().value` which treats ANY token as a column name. It never delegates to `parseExpr()` for boolean/comparison expressions.

## Fix Strategy: Route Through parseExpr
The cleanest fix would be to make parseSelectColumn use parseExpr for general expressions:
1. Try parseExpr() for the column expression
2. Check for AS alias after
3. Keep special cases only for things parseExpr doesn't handle (window functions, aggregates)

This is a major refactor but it's the right solution — the dual-path parsing is the source of all these bugs.

## Quick Fixes (tactical)
1. Add NULL/TRUE/FALSE literal handling before the `advance()` fallback
2. Add IS NULL/IS NOT NULL after column parsing
3. These are band-aids — the fundamental problem remains
