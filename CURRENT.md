# CURRENT.md — Active Work State

## Status: in-progress
## Session: Work Session B2 (Monday 4/13, 2:15 PM - 8:15 PM MDT)
## Current Position: T79
## Mode: MAINTAIN
## Task: Housekeeping
## Started: 2026-04-13T20:15:00Z
## Tasks Completed This Session: 55+
## Key Bugs Fixed (HenryDB):
- GROUP BY simple column alias resolution
- GROUP BY ordinal position support
- Histogram equality estimation for high-frequency values
- ACID result cache stale after DML
- Nested parenthesized expression parser
- NULLS FIRST/LAST parser (FIRST tokenized as KEYWORD)
- Correlated EXISTS (decorrelate table name vs alias)
- Arithmetic between aggregates (SUM/SUM)
## Key Bugs Fixed (Monkey):
- Let-binding compilation order
- Top-level return in VM
- String comparison (value-based, not reference)
## Key Features (Monkey):
- Bytecode instruction set (31 opcodes)
- Compiler (AST → bytecode)
- Stack VM (2.5x faster, TCO for unlimited recursion)
- while/for loops, set statement, modulo, string indexing
- type/str/int builtins
- REPL with --engine flag
- 298 tests
## Key Features (HenryDB):
- TPC-H micro-benchmark (Q1, Q6, Q14)
- 7 JSON functions with path navigation
- 27 JSON tests, 9 multi-join tests, 35 adversarial tests
- 18 GROUP BY tests, 14 NULLS/EXISTS tests
