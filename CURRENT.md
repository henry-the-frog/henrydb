# CURRENT.md

## Status: in-progress
## Session: B
## Date: 2026-04-18
## Time: 1:53 PM MDT
## Tasks Completed: ~30 (T100-T157)
## Builds: ~20
## Major Features: PG wire protocol (simple + extended), persistent server, parametric cost model, merge join, COPY, information_schema, SHOW TABLES/INDEXES/ALL, DESCRIBE
## Bugs Fixed: 4 (heap overflow, UNIQUE+HOT, column naming, Describe side effects)
## Next: SHOW CREATE TABLE column naming fix, TOAST overflow pages

## Context Files
- memory/2026-04-17.md
- memory/scratch/bug-patterns-2026-04-17.md
- lessons/integration-boundary-testing.md
- memory/scratch/ddl-wal-completeness.md

## Focus Projects
- henrydb
- neural-net

## Today's Goals
1. HenryDB: Secondary index + MVCC snapshot after UPDATE (HOT chains) — from TODO Normal
2. HenryDB: Fix file-wal.test.js double-logging issue — from TODO Normal
3. Neural-net: Diagnose and fix CI failures on main (ongoing since Apr 11)
4. Evening: PostgreSQL HOT chain research + stored procedure design exploration
