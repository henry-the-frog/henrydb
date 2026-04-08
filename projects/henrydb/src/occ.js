// occ.js — Optimistic Concurrency Control (validation-based)
// Three phases: (1) Read, (2) Validate, (3) Write
// During Read phase: reads/writes go to a local workspace (shadow copy)
// At commit: validate that no conflicts with other committed txns
// If valid: apply writes. If not: abort and retry.

export class OCC {
  constructor() {
    this._data = new Map(); // Global committed state
    this._txns = new Map(); // txnId → { readSet, writeSet, startTime, localCopy }
    this._commitHistory = []; // [{txnId, writeSet, commitTime}]
    this._clock = 0;
    this.stats = { commits: 0, aborts: 0, validations: 0 };
  }

  begin(txnId) {
    this._txns.set(txnId, {
      readSet: new Set(),
      writeSet: new Map(), // key → value
      startTime: ++this._clock,
    });
  }

  read(txnId, key) {
    const txn = this._txns.get(txnId);
    if (!txn) throw new Error(`Txn ${txnId} not found`);
    txn.readSet.add(key);
    // Read from local write set first, then global
    return txn.writeSet.has(key) ? txn.writeSet.get(key) : (this._data.get(key) ?? null);
  }

  write(txnId, key, value) {
    const txn = this._txns.get(txnId);
    if (!txn) throw new Error(`Txn ${txnId} not found`);
    txn.writeSet.set(key, value);
  }

  /**
   * Validate and commit. Returns { ok: true } or { ok: false, reason }.
   */
  commit(txnId) {
    const txn = this._txns.get(txnId);
    if (!txn) throw new Error(`Txn ${txnId} not found`);
    
    this.stats.validations++;

    // Validation: check that no transaction committed during our execution
    // that wrote to keys in our read set
    for (const history of this._commitHistory) {
      if (history.commitTime >= txn.startTime) {
        // This txn committed after we started — check for conflicts
        for (const key of history.writeSet) {
          if (txn.readSet.has(key)) {
            // Conflict: we read a key that was modified by a concurrent txn
            this._txns.delete(txnId);
            this.stats.aborts++;
            return { ok: false, reason: `Conflict on key '${key}' with txn ${history.txnId}` };
          }
        }
      }
    }

    // Also check write-write conflicts
    for (const history of this._commitHistory) {
      if (history.commitTime >= txn.startTime) {
        for (const key of history.writeSet) {
          if (txn.writeSet.has(key)) {
            this._txns.delete(txnId);
            this.stats.aborts++;
            return { ok: false, reason: `Write-write conflict on key '${key}'` };
          }
        }
      }
    }

    // Valid — apply writes
    const commitTime = ++this._clock;
    for (const [key, value] of txn.writeSet) {
      this._data.set(key, value);
    }

    this._commitHistory.push({
      txnId,
      writeSet: new Set(txn.writeSet.keys()),
      commitTime,
    });

    this._txns.delete(txnId);
    this.stats.commits++;
    return { ok: true, commitTime };
  }

  abort(txnId) {
    this._txns.delete(txnId);
    this.stats.aborts++;
  }

  getValue(key) { return this._data.get(key) ?? null; }
  getStats() { return { ...this.stats, activeTxns: this._txns.size }; }
}
