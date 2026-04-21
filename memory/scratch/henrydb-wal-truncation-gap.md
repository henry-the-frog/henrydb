# HenryDB WAL Truncation Gap Analysis
- created: 2026-04-20
- uses: 1
- tags: henrydb, wal, checkpoint, truncation

## Current State

### Three Database Classes
1. **Database** (`db.js`) — In-memory only. WAL is WALManager in-memory mode. `_checkpoint()` is a no-op that returns stats.
2. **PersistentDatabase** (`persistent-db.js`) — File-backed heaps + FileWAL. **No checkpoint. No truncation.**
3. **TransactionalDatabase** (`transactional-db.js`) — Full MVCC + FileWAL. **Has checkpoint + truncation. Auto-checkpoint at 16MB WAL.**

### WAL Truncation Status

| Class | Checkpoint | WAL Truncation | Auto-checkpoint |
|-------|-----------|---------------|-----------------|
| Database (in-memory) | fuzzyCheckpoint: truncates `_stableRecords` array | `truncate(beforeLsn)`: filters array | Via WALManager (record count) |
| PersistentDatabase | **NONE** | **NONE** | **NONE** |
| TransactionalDatabase | ✅ Full: flush heaps → save catalog → save MVCC → write checkpoint record → ftruncate WAL → re-write marker | ✅ ftruncateSync → 0 bytes | ✅ At 16MB WAL size |

### The Gap
**PersistentDatabase** uses FileWAL but never calls checkpoint or truncate. WAL grows indefinitely. The `FileWAL.truncate()` method exists (ftruncateSync) but is never called from PersistentDatabase.

### TransactionalDatabase's Approach (working correctly)
1. Verify no active transactions
2. `flush()` — write all heap pages to disk
3. `_saveCatalog()` + `_saveMvccState()` — persist metadata
4. `wal.checkpoint()` — write checkpoint record
5. `wal.truncate()` — ftruncateSync to 0 bytes
6. `wal.checkpoint()` — re-write marker for next recovery start

Auto-triggers when WAL >= 16MB (configurable).

### In-Memory WAL (WALManager)
- `fuzzyCheckpoint()` at L566: ARIES-style with dirty page table + active txns
  - Truncates `_stableRecords` to keep only records >= minRecLsn
  - Keeps checkpoint records even if before minRecLsn
- `truncate(beforeLsn)` at L638: simple filter on `_stableRecords`
- Problem: for non-inMemory mode, `truncate()` returns 0 (no-op)

### What Needs Fixing
1. **PersistentDatabase**: Add a `checkpoint()` method that:
   - Flushes all heaps to disk
   - Calls `this._wal.truncate()`
   - Re-writes checkpoint marker
   - Add auto-checkpoint by WAL size (copy pattern from TransactionalDatabase)

2. **WALManager (non-inMemory)**: `truncate()` should delegate to the underlying FileWAL writer's truncate, not return 0.

3. **Consider**: Should in-memory Database's `_stableRecords` grow unbounded? For long-running in-memory DBs with many writes, this array could get large. The fuzzyCheckpoint truncates it, but only if called explicitly or via the auto-checkpoint mechanism (record-count based).

### Risk Assessment
- **PersistentDatabase WAL growth**: P2 — most users use TransactionalDatabase. PersistentDatabase is simpler mode.
- **In-memory WAL growth**: P3 — in-memory DBs are typically short-lived (tests, demos)
- **TransactionalDatabase**: Working correctly. No issue.
