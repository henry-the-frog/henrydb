# DDL WAL Completeness Pattern

uses: 1
created: 2026-04-13
tags: henrydb, persistence, wal, ddl, bug-pattern

## The Pattern

Every DDL operation needs BOTH a write path (WAL logging) AND a read path (replay handling). Missing either creates silent data corruption:

| DDL Operation | Write Path | Read Path | Bugs Found |
|---|---|---|---|
| CREATE TABLE | ✅ | ✅ | (original) |
| CREATE INDEX | ✅ | ✅ | (original) |
| CREATE VIEW | ❌→✅ | ❌→✅ | Bug #14 |
| CREATE TRIGGER | ❌→✅ | ❌→✅ | Bug #15 |
| CREATE MATERIALIZED VIEW | ❌→✅ | ❌→✅ | (found with views) |
| DROP TABLE | ❌→✅ | ❌→✅ | Bug #16 |
| TRUNCATE TABLE | ❌→✅ | ❌→✅ | Bug #15 (Session B) |
| ALTER TABLE (all variants) | ❌→✅ | ❌→✅ | Bug #17 |
| DROP INDEX | ❌→✅ | ❌→✅ | (found with indexes) |

5 separate bug families from the same root cause: the DDL persistence allowlist only tracked CREATE TABLE and CREATE INDEX.

## Root Cause

Detection used `trimmed.startsWith('CREATE TABLE') || trimmed.startsWith('CREATE INDEX')` — a positive allowlist that rotted as new DDL types were added to the parser.

## The Fix

1. Changed to `trimmed.startsWith('CREATE ')` (catch-all for CREATE statements)
2. Added generic DDL WAL record type (12) for ALTER TABLE variants
3. Added specific WAL types for TRUNCATE (11) and DROP TABLE
4. `_trackAlter()` reconstructs CREATE TABLE SQL from current schema after ALTER

## Prevention Rule

**When adding a new DDL statement to the SQL parser, IMMEDIATELY add:**
1. WAL logging in the execution path
2. WAL replay handling in ALL recovery paths (there are 3 in HenryDB)
3. A persistence test: execute DDL → close → reopen → verify

**Negative > Positive:** Catch "all DDL except..." rather than "TABLE, INDEX, VIEW, ..." — new types are automatically covered.
