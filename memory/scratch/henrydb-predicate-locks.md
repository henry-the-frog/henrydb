# HenryDB: Predicate-Level SSI Locks (Design)

## Problem
Current SSI tracks reads at individual row level (`tableName:rowKey`). Scans create reads for ALL visible rows, causing false positives when concurrent transactions modify different rows.

## Solution: SIRead Lock Levels (PostgreSQL-inspired)

### Lock Granularity
1. **Tuple-level**: `tableName:rowKey` (current behavior, for point reads)
2. **Page-level**: `tableName:page:N` (for scans that touch specific pages)
3. **Table-level**: `tableName:*` (for full table scans)

### Lock Escalation
- Start with tuple-level locks
- If a transaction acquires > N tuple locks on same table, escalate to page-level
- If page locks > M, escalate to table-level
- Escalation increases false positives but reduces memory usage

### For Scans
- SELECT with WHERE that scans the table: record table-level lock (conservative but correct)
- SELECT with index lookup: record tuple-level lock (precise)
- SELECT with range scan: record page-level locks for pages touched

### For UPDATE/DELETE
- Record tuple-level locks only for rows actually modified (current fix)
- No need for scan-level locks since the WHERE filter is applied

### RW-Dependency Check
When checking for rw-dependencies:
- Table-level lock conflicts with any write to that table
- Page-level lock conflicts with writes to that page
- Tuple-level lock conflicts with writes to that specific row

### Implementation
1. Change `readSets` from `Map<txId, Set<key>>` to `Map<txId, LockSet>`
2. `LockSet` has three levels: tuples, pages, tables
3. `recordRead(txId, key, lockLevel)` — add to appropriate level
4. `recordWrite(txId, key)` — check all three levels for conflicts
5. Escalation logic in `recordRead` when tuple count exceeds threshold

### Tradeoffs vs Current Approach
- **Pro**: More accurate than row-level (handles phantoms via page/table locks)
- **Pro**: Memory-bounded via escalation
- **Con**: More false positives at higher lock levels
- **Con**: Need to determine "right" lock level for each query type

### vs Timestamp Cache (CockroachDB)
- Timestamp cache is simpler but designed for distributed systems
- SSI with lock levels is better fit for single-node system
- PostgreSQL uses SIRead locks; we should follow that model
