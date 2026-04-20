# SSI Fix: Suppress Reads During UPDATE Scans

uses: 1
created: 2026-04-20
tags: henrydb, ssi, mvcc, update, false-positive

## Problem
`UPDATE t SET val = x WHERE col = y` did a SeqScan reading ALL rows, recording
SSI read-locks for every row. When two concurrent UPDATEs touched disjoint rows
(e.g., id=1 vs id=3), the SSI falsely detected a dangerous structure because
both transactions appeared to have read-write conflicts on all rows.

## Root Cause
The MVCC scan interceptor (`transactional-db.js:444`) called `recordRead()` for
every visible row yielded during heap.scan(). UPDATE's WHERE filter runs AFTER
the scan yields rows, so SSI sees reads for rows that UPDATE never uses.

## Fix
1. Set `tx._suppressSsiReads = true` before UPDATE execution
2. Scan interceptor checks `!tx._suppressSsiReads` before calling recordRead
3. After UPDATE completes, reset `_suppressSsiReads = false`

## Why This Is Safe
- UPDATE's **writes** are still tracked (recordWrite still fires)
- Other transactions' reads of rows we write still create valid rw-dependencies
- The only reads suppressed are the scan-for-WHERE-match reads during UPDATE
- Explicit SELECT reads earlier in the transaction ARE still recorded
- Write skew detection still works because SELECT reads create the dependencies

## Key Insight
For SSI, UPDATE's WHERE-scan reads are noise — they don't represent real data
dependencies. The actual dependencies come from (a) explicit SELECT reads and
(b) the UPDATE's writes. Suppressing scan reads eliminates false positives
without weakening anomaly detection.

## Test Results
- Write skew STILL detected (SSI prevents it) ✓
- Disjoint transactions ALLOWED ✓  
- Bank transfers on disjoint accounts ALLOWED ✓
- Full suite: no regressions
