---
uses: 1
created: 2026-04-09
tags: database, architecture, sqlite, query-planner, bytecode
---

# SQLite Architecture Learnings

## Key Differences from HenryDB

1. **Bytecode vs AST interpretation**: SQLite compiles SQL → bytecode (VDBE opcodes), then executes. HenryDB walks AST directly. Bytecode is faster for repeated queries.

2. **B-Tree storage**: SQLite stores EVERYTHING in B-Trees — both tables and indexes. Each table is a B-Tree keyed by rowid. HenryDB uses heaps for tables, B+Trees only for secondary indexes.

3. **Query planner complexity**: SQLite's `where.c` + `wherecode.c` + `whereexpr.c` is thousands of lines of cost-based optimization. HenryDB's planner is minimal.

4. **Tokenizer calls parser** (not vice versa): SQLite's design has tokenizer feeding tokens to parser, which is more thread-safe than YACC's approach.

## Implications for HenryDB

- **JOIN performance**: Need a cost-based join optimizer that picks hash join vs nested loop based on table sizes
- **PK lookups**: Switch from heap scan to B-Tree organized table for faster primary key access
- **Prepared statements**: Could add a bytecode compilation step for hot queries
- **WAL**: SQLite's WAL mode is append-only with readers seeing consistent snapshots — similar to what HenryDB now has

## SQLite's WAL vs HenryDB's WAL
- SQLite: single WAL file, readers use shared memory for coordination
- HenryDB: ARIES-style with checkpoints, no shared memory (single-writer model)
- Both: readers don't block writers, writers don't block readers
