# CURRENT.md — Work Session Status

## Status: in-progress (Session B ending)

## Session B Summary (6:30-8:15 PM MDT)

### New modules (T231-T235)
- LZ77 compressor: 25x compression, binary serialization (11 tests)
- Connection pool: async waiters, health checks, idle/lifetime expiry (11 tests)
- Rate limiters: 5 algorithms — Token/Leaky Bucket, Sliding/Fixed Window, PerKey (16 tests)
- Thread pool: work-stealing deques, task dependencies, diamond DAG (10 tests)

### Critical bug fixes
- **QueryCache.extractTables** — ALL SELECTs through pg wire protocol broken. Fixed server.js + query-cache.js
- **DiskManager options constructor** — `{create:false}` parsed as `pageSize=NaN`, breaking ALL persistence ops
- **BufferPool API bridge** — null-disk guards, flushAll callback, fetchPage readFn, eviction tracking
- **TDigest O(n²) compression** — 94x speedup (100K: 13s → 142ms)

### Test improvements
| File | Before | After |
|------|--------|-------|
| server | 6/14 | 14/14 |
| persistent-db | 0/11 | 11/11 |
| crash-recovery | 0/9 | 9/9 |
| persistence-stress | 0/7 | 7/7 |
| file-wal | 0/8 | 8/8 |
| integration | 6/12 | 12/12 |
| LSM | 5/14 | 14/14 |
| edge-cases | 0/59 | 59/59 |
| tdigest | HANG | 8/8 |
| milestone-3000 | 19/24 | 24/24 |
| property-tests | 14/17 | 17/17 |
| regression-tests | 31/33 | 33/33 |

### Remaining known issues
- transactional-db/acid-compliance/bank-transfer: MVCCStore ≠ MVCCManager interface — needs real redesign
- integration-stress: 18/26 (same MVCC root cause)
