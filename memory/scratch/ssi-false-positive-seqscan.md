# SSI False Positive: SeqScan Reads Too Broadly

uses: 0
created: 2026-04-20
tags: henrydb, ssi, mvcc, predicate-locks

## Problem
When `UPDATE t SET val = 'x' WHERE id = 1` uses a SeqScan, it reads ALL rows
and records SIRead locks for every row. This causes SSI false positives when
two concurrent transactions update disjoint rows (e.g., id=1 vs id=3).

## Root Cause
The MVCC visibility check in transactional-db.js records a read for every
visible row yielded by the scan, not just the rows that match the WHERE clause.

## Solutions (increasing complexity)

### 1. Index-Backed UPDATE (Recommended First)
If WHERE clause references an indexed column, use IndexScan instead of SeqScan.
This reads only the matching rows, so SSI only tracks those.
**Effort**: Medium. Need to detect indexed WHERE in UPDATE path.
**Impact**: Fixes the most common false positive pattern.

### 2. Deferred Read Recording
Don't record reads during SeqScan. Instead, record reads only when a row
is actually used (matched WHERE, returned to caller).
**Effort**: Medium. Need to add a flag to suppress recording during scan.
**Impact**: Fixes all SeqScan-related false positives.

### 3. Predicate Locks (PostgreSQL approach)
Lock at page level, with escalation (tuple → page → relation).
**Effort**: High. Need page-level lock tracking, escalation logic.
**Impact**: Full SSI correctness with minimal false positives.

## PostgreSQL's Approach
- SIRead locks at page granularity (not row)
- Lock escalation: many tuple locks → page lock → relation lock
- Index gap locks for range queries
- Ports & Grittner, SIGMOD 2012

## Recommendation
Start with #1 (index-backed UPDATE) for the biggest immediate win.
Then #2 (deferred read recording) for remaining cases.
Predicate locks (#3) only if false positives remain problematic.
