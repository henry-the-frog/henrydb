# Savepoint Physicalization: In-Memory Markers Must Reach Disk

uses: 1
created: 2026-04-13
tags: henrydb, mvcc, persistence, savepoint, bug-pattern

## The Bug

Savepoint-revoked rows used `xmax = -2` as an in-memory-only sentinel meaning "permanently dead, don't resurrect." But after close/reopen, recovery reset all xmax values to 0 (the "alive" default), resurrecting the dead rows.

## Root Cause

The `xmax = -2` marker existed only in the in-memory version map. It was never written to WAL or heap pages. Recovery had no way to know these rows should be dead.

## The Fix

During transaction COMMIT, physically DELETE rows with `xmax = -2` from the heap before writing the commit WAL record. This way, the WAL contains both the INSERT and the compensating DELETE, and recovery replays both correctly.

## General Principle

**Any in-memory state marker that affects data visibility MUST be physicalized before the data reaches durable storage.** Recovery can only reconstruct what's in the WAL + heap. If a state transition exists only in RAM, it will be lost on crash/restart.

## Checklist for New MVCC State

When adding a new transaction state marker:
1. Is it persisted in the WAL? If no, it's lost on crash.
2. Is it persisted in the heap page? If no, it's lost on eviction.
3. Can recovery reconstruct it from WAL replay alone?
4. Test: `BEGIN → operation → COMMIT → close → reopen → verify`
