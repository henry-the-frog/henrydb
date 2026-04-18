# PostgreSQL vs HenryDB MVCC Visibility Comparison

uses: 0
created: 2026-04-18
tags: database, mvcc, visibility, postgresql, comparison

## PostgreSQL Visibility Model

### Data Structures (per tuple)
- `xmin` — XID of creating transaction
- `xmax` — XID of deleting/updating transaction (0 = not deleted)
- `t_infomask` — bitmask with hint bits, lock flags

### HeapTupleSatisfiesMVCC Logic
1. Check `xmin` status (committed? aborted? still running?)
2. Check `xmax` status
3. Compare against snapshot's range: `[snapshot.xmin, snapshot.xmax)`
4. Check `snapshot.xip` (in-progress transaction list)
5. Use hint bits to avoid clog lookups

### Snapshot Contents
- `xmin` — oldest active txn at snapshot time (anything < this is definitely committed)
- `xmax` — first unassigned XID (anything >= this is definitely not visible)
- `xip[]` — list of in-progress txns within [xmin, xmax)

### Visibility Rule (simplified)
```
tuple visible IF:
  xmin is committed before snapshot AND
  (xmax == 0 OR xmax is aborted OR xmax >= snapshot.xmax OR xmax in xip[])
```

## HenryDB Visibility Model

### Data Structures
- `_versions` Map: `"pageId:slotIdx"` → `{xmin, xmax, xminCommitted, xmaxCommitted}`
- Per-version hint bits (lazy, like PG)

### Visibility Logic (MVCCHeap.scan)
```js
const created = _isVisibleWithHints(ver, 'xmin', tx);
const deleted = ver.xmax !== 0 && _isVisibleWithHints(ver, 'xmax', tx);
if (created && !deleted) yield row;
```

### _isVisibleWithHints
1. Fast path: hint bit set → committed
2. Slow path: check mgr.committedTxns and mgr.isVisible()
3. Sets hint bit on first determination

### Key Differences from PostgreSQL

| Aspect | PostgreSQL | HenryDB |
|--------|-----------|---------|
| Version storage | In tuple header | Separate Map |
| Hint bits | In tuple t_infomask | In version entry |
| Snapshot | xmin, xmax, xip[] | startTx (simplified) |
| Abort handling | pg_clog checks | committedTxns Set |
| Multixact | Full support | Not implemented |
| Row locks | HEAP_XMAX_IS_LOCKED_ONLY | Not implemented |
| Subtransactions | Full savepoint support | Basic savepoints |
| xid wraparound | Handled via epoch | Not an issue (JS numbers) |

### Gap Analysis
1. **Snapshot simplification**: HenryDB uses only `startTx` without an explicit in-progress list.
   This means it can't distinguish between "committed before snapshot" and "committed after
   snapshot started but before read". PG's xip[] handles this correctly.
   
2. **No CLOG**: HenryDB tracks committed txns in a Set. PG uses a persistent commit log
   (pg_clog/pg_xact) that survives restarts.

3. **No row-level locking via xmax**: PG uses xmax with HEAP_XMAX_IS_LOCKED_ONLY for
   SELECT FOR UPDATE. HenryDB doesn't support this.

4. **No frozen tuples**: PG marks very old tuples as "frozen" (visible to everyone) to
   prevent xid wraparound issues. HenryDB doesn't need this due to JS number range.

## Potential Improvements for HenryDB
1. Add in-progress transaction list to snapshots for correct concurrent visibility
2. Add SELECT FOR UPDATE (xmax-based locking)
3. Add persistent commit log for crash recovery of visibility state
