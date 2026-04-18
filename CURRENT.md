# CURRENT.md

## Status: session-ended
## Session: B (afternoon)
## Date: 2026-04-18
## Time: 1:57 PM MDT
## Tasks Completed: ~35 (T100-T157)
## Builds: ~22
## Major Features: PG wire protocol (simple + extended), persistent server, parametric cost model, merge join, COPY FROM/TO, information_schema, SHOW TABLES/INDEXES/ALL/CREATE TABLE, DESCRIBE, SET cost params
## Bugs Fixed: 4 (CRITICAL heap overflow, UNIQUE+HOT, column naming, Describe side effects)
## Test Count: 200+ pass, 0 fail across 13 test files
## Next Session Focus: SHOW CREATE TABLE column fix was last item. TOAST overflow pages, system catalog tables.

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
