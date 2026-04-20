# 2026-04-20-stress-testing.md
## Lessons from TPC-H stress testing and differential fuzzing

### Feature Theater
Building capabilities that aren't wired into the execution path. HenryDB has a full cost-based optimizer with DP enumeration, hash join, and merge join implementations — but the executor always does nested loop. The planner's output is shown in EXPLAIN but never used for actual execution. This went undetected across hundreds of tests because unit tests test individual features in isolation — they don't test the full query pipeline end-to-end with realistic data.

**Rule**: After building any optimization or execution strategy, write an end-to-end test that proves it's actually used (not just that it works in isolation).

### JavaScript Numeric Model vs SQL Types
JavaScript's `Number.isInteger(10.0)` returns `true` because `10.0 === 10` in JS. This means `parseFloat("10.0")` returns `10` which passes the integer check. The SQL division `10.0 / 3` should return `3.33` (REAL) but returns `3` (INTEGER truncated) because the tokenizer loses the `.0` type information.

**Rule**: When implementing SQL types in JavaScript, tag values with their SQL source type at parse time. Don't rely on JS runtime type checks.

### Dual Expression Parsing Paths
HenryDB's parser has `parseExpr()` for WHERE clauses and custom hand-coded logic in `parseSelectColumn()` for SELECT columns. They support different subsets of SQL. `IS NULL` works in WHERE but not in SELECT. `NULL` literal works in WHERE but becomes a column name in SELECT.

**Rule**: A parser should have one expression parsing path, used everywhere. Duplicating expression handling guarantees inconsistency.

### Differential Fuzzing as Quality Gate
A 48-query differential fuzzer against SQLite found 4 bugs in 30 seconds that 4204 unit tests missed. Unit tests verify what you expect; differential fuzzing verifies what you don't expect. The division bug has been present since the beginning and affects every revenue calculation.

**Rule**: After reaching feature completeness in a domain, run differential tests against a reference implementation before adding more features.
