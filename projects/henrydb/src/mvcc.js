// mvcc.js — Multi-Version Concurrency Control
// Each row maintains a version chain. Transactions see a snapshot.
// Used in PostgreSQL, MySQL InnoDB, Oracle.

export class MVCCStore {
  constructor() {
    this._versions = new Map(); // key → [{value, txId, deleted}]
    this._nextTx = 1;
    this._activeTxns = new Map(); // txId → {startTx, writes}
  }

  /** Begin a new transaction. Returns txId. */
  begin() {
    const txId = this._nextTx++;
    this._activeTxns.set(txId, { startTx: txId, writes: [], committed: false });
    return txId;
  }

  /** Read a key at a transaction's snapshot. */
  read(txId, key) {
    const versions = this._versions.get(key);
    if (!versions) return undefined;
    
    const tx = this._activeTxns.get(txId);
    // Find the latest version visible to this transaction
    for (let i = versions.length - 1; i >= 0; i--) {
      const v = versions[i];
      if (v.txId < tx.startTx || v.txId === txId) {
        // Check if the writing transaction committed
        const writerTx = this._activeTxns.get(v.txId);
        if (v.txId === txId || !writerTx || writerTx.committed) {
          return v.deleted ? undefined : v.value;
        }
      }
    }
    return undefined;
  }

  /** Write a key in a transaction. */
  write(txId, key, value) {
    if (!this._versions.has(key)) this._versions.set(key, []);
    this._versions.get(key).push({ value, txId, deleted: false });
    this._activeTxns.get(txId).writes.push(key);
  }

  /** Delete a key in a transaction. */
  delete(txId, key) {
    if (!this._versions.has(key)) this._versions.set(key, []);
    this._versions.get(key).push({ value: undefined, txId, deleted: true });
    this._activeTxns.get(txId).writes.push(key);
  }

  /** Commit a transaction. */
  commit(txId) {
    const tx = this._activeTxns.get(txId);
    tx.committed = true;
  }

  /** Rollback: remove all versions written by this transaction. */
  rollback(txId) {
    const tx = this._activeTxns.get(txId);
    for (const key of tx.writes) {
      const versions = this._versions.get(key);
      if (versions) {
        const filtered = versions.filter(v => v.txId !== txId);
        this._versions.set(key, filtered);
      }
    }
    this._activeTxns.delete(txId);
  }

  /** Garbage collect: remove old versions not needed by any active transaction. */
  gc() {
    const minActive = Math.min(...[...this._activeTxns.keys()]);
    let cleaned = 0;
    for (const [key, versions] of this._versions) {
      const visible = versions.filter(v => v.txId >= minActive || v.txId === versions[versions.length - 1].txId);
      cleaned += versions.length - visible.length;
      this._versions.set(key, visible);
    }
    return cleaned;
  }

  getStats() {
    let totalVersions = 0;
    for (const [, versions] of this._versions) totalVersions += versions.length;
    return {
      keys: this._versions.size,
      totalVersions,
      activeTxns: this._activeTxns.size,
    };
  }
}
