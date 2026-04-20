# Savepoint ROLLBACK TO Isolation Bug

uses: 1
created: 2026-04-20
tags: henrydb, savepoint, mvcc, isolation, concurrency

## Bug
ROLLBACK TO modified the **shared** version map for ALL rows, not just the
rolling-back transaction's rows. This caused other sessions' rows to become
invisible after an unrelated savepoint rollback.

## Root Cause
The version map (`_versionMaps`) is global — shared between all sessions.
When s1 did `ROLLBACK TO sp`, it checked which keys were NOT in the savepoint
snapshot and marked them as dead (xmax=-2). But keys inserted by s2 weren't
in s1's snapshot either, so they also got marked as dead.

## Fix
Two-pass version map restoration:
1. **Revoke pass**: Only mark keys as dead if `ver.xmin === myTxId`
2. **Restore pass**: 
   - Own rows (xmin === myTxId): full state restore
   - Other txns' rows: only restore xmax if we changed it (handles UPDATE undo)
   - Missing rows: restore from snapshot

## Key Insight
Shared mutable state (version maps) + concurrent modifications = isolation bugs.
Any operation that modifies shared state must filter by transaction ownership.
This is the same class of bug as "shared lock tables" in traditional DBMS —
the fix is always "scope modifications to the current transaction."

## Pattern
**Always check transaction ownership before modifying shared MVCC state.**
