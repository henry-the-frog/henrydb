# Scratch Notes: HenryDB Persistence Bugs & Lessons

**Created: 2026-04-09 | Uses: 1**

## Critical Bugs Found

### Bug #1: BufferPool.fetchPage ignores disk read callback
- `fetchPage(pageId)` was the signature — no callback parameter
- FileBackedHeap called `bp.fetchPage(pid, (p) => dm.readPage(p))` — second arg silently ignored
- Result: all pages loaded as zeroed buffers on reopen
- Fix: add optional `diskReadFn` parameter to fetchPage

### Bug #2: BufferPool.flushAll ignores evict callback  
- `flushAll()` always called `this._writeToDisk(pid, data)` which writes to an in-memory Map
- FileBackedHeap called `bp.flushAll((pid, data) => dm.writePage(pid, data))` — callback ignored
- Result: dirty pages never written to actual disk files
- Fix: flushAll accepts writeCallback, falls back to evictCallback, then _writeToDisk

### Bug #3: In-memory disk simulation masks real I/O bugs
- `_readFromDisk(pageId)` returns `Buffer.alloc(PAGE_SIZE)` (zeroed) for unknown pages
- `_writeToDisk(pageId, data)` writes to `this._disk` (a Map), not real disk
- These "simulators" meant the BufferPool worked perfectly in-memory tests but completely failed for real file I/O

## Lessons

1. **Layer mismatches are silent killers.** The BufferPool was designed as simulation-first. When FileBackedHeap tried to use it for real I/O, the mismatched APIs silently degraded to zeros.

2. **Test the actual persistence path.** 3000+ tests passed but none tested close-and-reopen with file-backed pages. The in-memory path was thoroughly tested; the file path was completely broken.

3. **Callbacks that get silently ignored are a footgun.** JavaScript doesn't warn when you pass extra args to a function that doesn't use them.

4. **WAL recovery works when the heap is empty.** Truncating page files to 0 bytes, then replaying WAL, correctly recovers all committed data. ARIES-style recovery is solid.

5. **Auto-commit inserts use txId=0.** The DB-level BEGIN/COMMIT doesn't propagate to the WAL layer's transaction IDs. All autocommit statements share txId=0 and have no explicit COMMIT record. This means WAL recovery of autocommit data relies on the data being in page files, not on WAL COMMIT records.

## Architecture Understanding

```
PersistentDatabase
  └─ Database (query engine)
       └─ HeapFile (in-memory pages) ← used by most tests
  └─ FileBackedHeap (file-backed pages) ← for persistence
       └─ BufferPool (page cache with eviction)
            └─ DiskManager (raw page I/O)
       └─ FileWAL (write-ahead log on disk)
            └─ ARIES recovery: analysis → redo phases
```

The key insight: `Database` uses `HeapFile` (in-memory). `PersistentDatabase` replaces it with `FileBackedHeap` via a `heapFactory` callback. The BufferPool sits between them, but was designed for the in-memory path only.
