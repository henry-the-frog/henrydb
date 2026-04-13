# HenryDB Persistence — Consolidated Notes

uses: 2
created: 2026-04-09
last-used: 2026-04-12
tags: database, persistence, buffer-pool, wal, recovery, pagelsn, mvcc

## BufferPool Layer Mismatch Bugs (Apr 9)

### Bug #1: fetchPage ignores disk read callback
- JS silently ignores extra args → `diskReadFn` callback never called → zeroed pages on reopen

### Bug #2: flushAll ignores evict callback
- Always wrote to in-memory Map → dirty pages never persisted to actual files

### Bug #3: In-memory disk simulation masks real I/O
- `_readFromDisk` returns zeroed buffer for unknowns, `_writeToDisk` writes to a Map
- 3000+ tests passed, ZERO tested close→reopen with file-backed pages

**Lesson:** Test the actual persistence path. Layer mismatches are silent killers in JS.

## MVCC + Persistence Interaction (Apr 11)

1. **Dead rows survive close/reopen** — version map discarded, recovery treats all rows as live. Fix: `_compactDeadRows()` on close.
2. **Savepoint rollback rows resurrected** — WAL still has INSERT after rollback. Fix: compensating DELETE WAL records.
3. **PK index not rebuilt after recovery** — WHERE fails, full scans work. Fix: rebuild from heap scan.

**Pattern:** Bugs live at the boundary between in-memory state and on-disk state.

## PageLSN Implementation Path (Apr 11, IMPLEMENTED)

Per-page LSN in page header → per-page redo decisions → idempotent recovery by construction.
Eliminates: lastAppliedLSN hack, full-vs-incremental heuristic, "wipe all pages" recovery.
~50-80 lines. Page header: 16 → 24 bytes (add BigUint64 for pageLSN).
Enables parallel recovery (pages independent).

## Architecture
```
PersistentDatabase
  └─ Database (query engine)
       └─ HeapFile (in-memory pages)
  └─ FileBackedHeap (file-backed pages)
       └─ BufferPool (page cache with eviction)
            └─ DiskManager (raw page I/O)
       └─ FileWAL (write-ahead log on disk)
            └─ ARIES recovery: analysis → redo phases
```
