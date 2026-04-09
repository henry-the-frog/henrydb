# Failures & Patterns

## 2026-04-09 HenryDB Depth Day

### Critical Bugs Found and Fixed
1. **ALL SELECT queries through server were broken** — `QueryCache.extractTables` was missing, and `set()` args were swapped. Every SELECT threw an error.
2. **O(n²) WAL flush** — `_flushToStable()` used `Array.includes()` (O(n) per record). Made INSERT 8x slower at scale. Fixed with index tracking.
3. **7 test files had ghost imports** — WAL_TYPES, WALRecord, recoverFromWAL, recoverToTimestamp didn't exist. Tests were never running.
4. **LSM tree API mismatches** — `insert()` vs `set()`, `find()` vs `get()`, `!== null` vs `!== undefined`. LSM was completely non-functional.
5. **Cuckoo hash operator precedence** — `>>> 0 % capacity` parsed as `>>> (0 % capacity)`. Hash function always returned raw hash, never modulo capacity.
6. **PlanCache LRU used Date.now()** — sub-millisecond operations all got same timestamp, making LRU random. Fixed with monotonic counter.
7. **DeadlockDetector API drift** — `registerTxn` vs `registerTransaction`, `addWait` vs `recordWait`.

### Pattern: Ghost Exports
Tests and source were generated in different passes. Tests assume APIs that don't match implementations:
- WAL: 7 files importing nonexistent exports
- LSM: 3 method name mismatches
- Fix pattern: add aliases at export level, don't rename existing code

### Lesson: The O(n²) Bug
The WAL flush bug was invisible at small scale. Always benchmark at 10x-100x expected scale.

## 2026-04-08
- **Dashboard API routes 404** — Server runs on port 3000, responds to requests, but archive-day and regenerate endpoints return {"error":"Not found"}. Server was rebuilt from scratch this morning — likely route naming mismatch between generate.cjs expectations and new server.js routes.
- **Knowledge system underutilized** — 468 BUILD tasks today but only 1 reference to lessons/failures in daily log. THINK/PLAN tasks didn't consult failures.md. Pattern: high-velocity build sessions skip knowledge feedback loops.

## 2026-04-07
- **Dashboard server down** — port 3000 unreachable during both MAINTAIN tasks (T4 and evening review). Archive-day and regenerate both failed. Cause unknown — server may not have been restarted after last reboot. This is 2nd occurrence (also failed during Session C part 2 MAINTAIN). Pattern: dashboard server doesn't auto-start.
