# SSI False Positive — Sequential Transactions

uses: 1
created: 2026-04-20
tags: henrydb, ssi, mvcc, concurrency

## Bug
SSI `recordWrite()` checked all readSets for rw-dependencies, including readSets from transactions that **committed before the current transaction started**. This created false rw-antidependencies between sequential (non-overlapping) transactions.

## Root Cause
`recordWrite` iterated `this.readSets` without checking if the reader was concurrent. A committed reader whose txId < writer's snapshot.xmin is NOT concurrent — they ran sequentially, and sequential transactions can never cause serialization anomalies.

## Fix
```javascript
// Skip if otherTx committed before our snapshot started (non-concurrent)
if (writerTx?.snapshot && this.committedTxns.has(otherTxId) &&
    otherTxId < writerTx.snapshot.xmin) {
  continue;
}
```

## Key Insight
SSI's dangerous structure detection (T_in →rw→ T →rw→ T_out) is only meaningful when the transactions **overlap in time**. The PostgreSQL SSI paper (Ports & Grittner 2012) is explicit about this: rw-antidependencies only exist between concurrent transactions. Our `recordRead` already had snapshot-based filtering, but `recordWrite` didn't.

## Related
- `recordRead` was correct — it checks `committedTxId >= readerSnap.xmax || readerSnap.activeSet.has(committedTxId)`
- The asymmetry between recordRead and recordWrite's concurrency checks was the bug
