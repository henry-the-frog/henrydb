# SQL Arithmetic Operator Precedence Bug

uses: 1
created: 2026-04-14
tags: henrydb, parser, sql, bug-pattern, critical

## The Bug

`parsePrimaryWithConcat()` handled ALL arithmetic operators (`+`, `-`, `*`, `/`, `%`) at the same precedence level, left-to-right. This meant:

- `2 * 5 + 2 * 3` → `((2 * 5) + 2) * 3 = 36` instead of `(2 * 5) + (2 * 3) = 16`
- `WHERE price + tax * rate > 100` → evaluates as `(price + tax) * rate` instead of `price + (tax * rate)`

This is a **data corruption bug** — every compound arithmetic expression in WHERE clauses, CHECK constraints, expression indexes, generated columns, and computed SELECT columns was potentially producing wrong results.

## Root Cause

The original parser had a single loop: `parsePrimary → while (+ - * / %) → parsePrimary`. No precedence hierarchy. All operators bind left-to-right at the same level.

## The Fix

Split into two levels:
1. `parsePrimaryWithConcat()` → `parseMultiplicative()` for additive ops (+, -, ||)
2. `parseMultiplicative()` → `parsePrimary()` for multiplicative ops (*, /, %)

Higher-precedence operators (*, /, %) bind first in `parseMultiplicative()`, then lower-precedence operators (+, -) bind in `parsePrimaryWithConcat()`.

## Scope of Impact

Every code path that calls `parseExpr()` was affected:
- WHERE clauses
- CHECK constraints
- Expression indexes
- Generated columns
- HAVING clauses
- ON join conditions
- ORDER BY expressions
- LIMIT/OFFSET expressions

## Prevention

- **Precedence test suite**: `SELECT 2 + 3 * 4` should return 14, not 20
- **Differential testing against SQLite** would have caught this immediately
- Standard parser architecture: separate parse functions per precedence level

## Meta-Lesson

This is the #1 parser bug: building arithmetic parsing as a flat loop. The correct approach is ALWAYS to have separate functions per precedence level (or use a table-driven Pratt parser). Every simple "just handle operators in a while loop" implementation will get precedence wrong unless it has explicit precedence handling.

The fact that existing tests didn't catch this means all test expressions were either single-operator (`a + b`) or happened to evaluate correctly with left-to-right flat precedence. **Whenever you fix a precedence bug, immediately add multi-operator expressions to the test suite that would fail under flat precedence.**
