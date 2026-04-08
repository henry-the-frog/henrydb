// timestamp-ordering.js — Timestamp Ordering (TO) concurrency control
// Each transaction gets a timestamp on begin.
// Each data item tracks read-timestamp (R-TS) and write-timestamp (W-TS).
// Rules:
//   Read: if TS(T) < W-TS(X), abort T (trying to read a value overwritten by a newer txn)
//   Write: if TS(T) < R-TS(X), abort T (trying to overwrite a value already read by newer txn)
//          if TS(T) < W-TS(X), skip (Thomas Write Rule: older write is obsolete)
// This provides serializability without locking.

export class TimestampOrdering {
  constructor() {
    this._clock = 0;
    // data item → { readTS, writeTS, value }
    this._items = new Map();
    // txnId → { ts, reads: Set, writes: Map<key,value> }
    this._txns = new Map();
    this.stats = { commits: 0, aborts: 0, reads: 0, writes: 0, thomasSkips: 0 };
  }

  begin(txnId) {
    const ts = ++this._clock;
    this._txns.set(txnId, { ts, reads: new Set(), writes: new Map(), aborted: false });
    return ts;
  }

  read(txnId, key) {
    const txn = this._txns.get(txnId);
    if (!txn || txn.aborted) throw new Error(`Txn ${txnId} not active`);

    const item = this._getItem(key);

    // If a newer transaction has already written this item, abort
    if (txn.ts < item.writeTS) {
      this._abort(txnId);
      return { ok: false, reason: 'read too late' };
    }

    // Update read timestamp
    item.readTS = Math.max(item.readTS, txn.ts);
    txn.reads.add(key);
    this.stats.reads++;

    // Return the committed value or the txn's own write
    const value = txn.writes.has(key) ? txn.writes.get(key) : item.value;
    return { ok: true, value };
  }

  write(txnId, key, value) {
    const txn = this._txns.get(txnId);
    if (!txn || txn.aborted) throw new Error(`Txn ${txnId} not active`);

    const item = this._getItem(key);

    // If a newer transaction has already read this item, abort
    if (txn.ts < item.readTS) {
      this._abort(txnId);
      return { ok: false, reason: 'write too late (read dependency)' };
    }

    // Thomas Write Rule: if a newer txn already wrote, skip this write
    if (txn.ts < item.writeTS) {
      this.stats.thomasSkips++;
      return { ok: true, skipped: true };
    }

    txn.writes.set(key, value);
    this.stats.writes++;
    return { ok: true };
  }

  commit(txnId) {
    const txn = this._txns.get(txnId);
    if (!txn || txn.aborted) return false;

    // Apply all writes
    for (const [key, value] of txn.writes) {
      const item = this._getItem(key);
      item.value = value;
      item.writeTS = txn.ts;
    }

    this._txns.delete(txnId);
    this.stats.commits++;
    return true;
  }

  _abort(txnId) {
    const txn = this._txns.get(txnId);
    if (txn) {
      txn.aborted = true;
      this._txns.delete(txnId);
      this.stats.aborts++;
    }
  }

  abort(txnId) { this._abort(txnId); }

  _getItem(key) {
    if (!this._items.has(key)) {
      this._items.set(key, { readTS: 0, writeTS: 0, value: null });
    }
    return this._items.get(key);
  }

  getValue(key) {
    const item = this._items.get(key);
    return item ? item.value : null;
  }

  getStats() { return { ...this.stats, activeItems: this._items.size, activeTxns: this._txns.size }; }
}
