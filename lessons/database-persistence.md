# Database Persistence — Lessons

_Promoted from scratch/henrydb-persistence.md (uses: 2, Apr 9→12)_

## BufferPool Layer Mismatch Bugs
- `fetchPage()` ignores disk read callback — second arg silently ignored, all file-backed pages loaded as zeros
- `flushAll()` ignores evict callback — always writes to in-memory Map, dirty pages never persisted
- `_readFromDisk()` returns zeros for unknown pages instead of throwing — masks real I/O bugs

**Root cause:** BufferPool designed for in-memory simulation. JS doesn't warn about extra args. Integration tests that cross layer boundaries are the only way to catch these.

## Recovery Rules
1. Recovery creates "fresh" derived objects (indexes, caches) but doesn't populate from restored data → every derived structure needs explicit rebuild
2. PK enforcement was never wired for basic INSERTs — only worked by accident because in-memory tables always had populated indexes at insert time
3. Recovered rows with synthetic txId (e.g., 0) need special MVCC visibility rules — treat as "frozen" (always visible)
4. `heap._rowCount` must be reset before recovery replay or rows get double-counted
5. `lastAppliedLSN` must be persisted — in-memory-only LSN tracking means recovery can't distinguish "needs replay" from "already applied"

## The Close→Reopen Test
3000+ tests passed but ZERO tested close→reopen→read-back with file-backed pages. The entire persistence layer was non-functional. **Always test the full lifecycle.**

## MVCC State Persistence
After checkpoint + WAL truncation + reopen, DELETE operations were lost because version maps (xmin/xmax) weren't persisted. Recovery rebuilt from heap scan → all physical rows visible, including deleted ones. Fix: persist version maps alongside data, or use in-heap markers.

## WAL + In-Place Modification Tension
WAL record-based recovery + in-place data modification = fundamental conflict. Recovery replays WAL expecting empty pages, but in-place modifications create data not in WAL. Fix: checkpoint to establish boundaries, or put everything in WAL.
