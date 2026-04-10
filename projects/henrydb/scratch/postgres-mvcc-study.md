# PostgreSQL MVCC Internals — Study Notes (Apr 9, 2026)

## Source
Hironobu SUZUKI, "The Internals of PostgreSQL" Chapter 5
https://www.interdb.jp/pg/pgsql05/

## Key Architecture Differences: PostgreSQL vs HenryDB

### Transaction IDs
- **PG**: 32-bit unsigned int, wraps around after ~4.2B. Has "freeze" mechanism.
- **HenryDB**: Simple incrementing counter, no wraparound handling.

### Tuple Structure  
- **PG**: `HeapTupleHeaderData` — 23 bytes overhead per tuple. Contains:
  - `t_xmin` (inserting txid)
  - `t_xmax` (deleting/locking txid)  
  - `t_cid` (command ID within transaction)
  - `t_ctid` (pointer to newer version — forms update chain!)
  - `t_infomask` (hint bits for caching commit status)
- **HenryDB**: Separate version map `{xmin, xmax}` alongside heap rows.
  - **Gap**: No `t_ctid` update chains, no command ID, no hint bits.

### Snapshot Representation
- **PG**: `xmin:xmax:xip_list` — e.g., `100:104:100,102`
  - `xmin`: lowest active txid. All below are committed/aborted.
  - `xmax`: first unassigned txid. All at/above haven't started.
  - `xip_list`: explicit list of active txids between xmin and xmax.
- **HenryDB**: Just `startTx` (the txid assigned to this transaction).
  - Checks `writerTx.commitTxId <= tx.startTx` for visibility.
  - **Gap**: Can't handle the case where txid 101 committed but 100 is still active.
  
### Visibility Rules
PG has 10+ rules covering every combination:
1. `t_xmin == IN_PROGRESS && t_xmin == current_txid` → check t_cid
2. `t_xmin == IN_PROGRESS && t_xmin == current_txid && t_xmax == INVALID` → Visible
3. `t_xmin == IN_PROGRESS && t_xmin == current_txid && t_xmax == current_txid` → Invisible
4. `t_xmin == IN_PROGRESS && t_xmin ≠ current_txid` → Invisible
5. `t_xmin == COMMITTED && Snapshot(t_xmin) == active` → Invisible
6. `t_xmin == COMMITTED && t_xmax == INVALID` → Visible
7-10: Various combinations of committed xmax with snapshot status.

**Key insight**: Rule 5 is how REPEATABLE READ prevents phantom reads.
Rule 9 vs 10: The snapshot determines if a committed xmax is "active" or not.

### Hint Bits (Performance Optimization)
- PG caches commit status IN THE TUPLE HEADER via `t_infomask` bits.
- Flags: `HEAP_XMIN_COMMITTED`, `HEAP_XMIN_INVALID`, `HEAP_XMAX_COMMITTED`, `HEAP_XMAX_INVALID`
- Avoids repeated clog lookups.
- **HenryDB**: No hint bits. Checks `committedTxns` set each time.

### Commit Log (clog)
- PG: Shared memory array, 2 bits per txid (IN_PROGRESS/COMMITTED/ABORTED/SUB_COMMITTED).
- Persisted to `pg_xact/` on checkpoint.
- **HenryDB**: `committedTxns` Set + `activeTxns` Map. No persistence of commit status.

### READ COMMITTED vs REPEATABLE READ
- **PG READ COMMITTED**: New snapshot per statement.
- **PG REPEATABLE READ**: Snapshot at first command, reused for all subsequent.
- **HenryDB**: Only REPEATABLE READ (snapshot at begin). No READ COMMITTED.

## What HenryDB Should Add (Priority Order)

1. **Proper snapshot representation** (xmin:xmax:xip_list)
   - Our `startTx` approach fails when txids commit out of order
   - Fix: Record active txids at snapshot time

2. **Update chains** (t_ctid)
   - PG links old→new tuples for fast version traversal
   - Our approach uses separate version map (works but no chain)

3. **Hint bits** (performance)
   - Cache commit status on tuple header
   - Huge speedup for repeated scans

4. **Commit log persistence**
   - Currently committedTxns is in-memory only
   - Need pg_xact equivalent for crash recovery

5. **READ COMMITTED isolation**
   - Useful default (PG's default)
   - New snapshot per statement

## Surprising Findings
- PG's UPDATE = DELETE + INSERT (same as HenryDB!) with version chain pointer
- t_cid handles "same-txn visibility" (INSERT then UPDATE in same txn)
- Hint bits are a dirty-write optimization — set lazily when checking visibility
- PG prevents phantom reads in REPEATABLE READ (not just non-repeatable reads)
- The snapshot xip_list is key — without it, out-of-order commits break visibility
