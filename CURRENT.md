# CURRENT.md — Work Session Status

## Status: session-ended

## Last Session: B (Thu Apr 9 evening, 6:30-8:00 PM MDT)

### Tasks completed: T231-T252 (22 tasks)

### Highlights
1. **Critical persistence stack fix** — DiskManager options constructor bug broke ALL file-backed operations. Fixed BufferPool/FileBackedHeap API bridge. persistent-db 0→11/11, crash-recovery 0→9/9, file-wal 0→8/8.
2. **TDigest 94x performance fix** — O(n²) → O(n log n) compression.
3. **Server QueryCache regression fix** — ALL SELECTs via pg wire protocol were broken.
4. **Systematic ghost API cleanup** — SkipList, RingBuffer, BitmapIndex, HLL, CMS aliases added. 10+ test files fixed.

### Test scorecard (key files)
- server 14/14, SQL 46/46, adaptive 10/10, buffer-pool 14/14
- persistent-db 11/11, crash-recovery 9/9, file-wal 8/8, persistence-stress 7/7
- integration 12/12, edge-cases 59/59, tdigest 8/8, milestone-3000 24/24
- LSM 14/14, property-tests 17/17, regression-tests 33/33

### Known issues for next session
- transactional-db/MVCC: MVCCStore ≠ MVCCManager interface mismatch
- sequences.test.js: off-by-one in setval with isCalled=false  
- server-auth.test.js: password auth config issues
- query-rewriter.test.js: 2 AST matching failures
