// mv-index.js — MVCC-aware index with version chains
// Each key maps to a chain of versions, each tagged with txn timestamps.

export class MVIndex {
  constructor() {
    this._data = new Map(); // key → [{value, txnId, beginTs, endTs, deleted}]
  }

  /** Insert a new version */
  insert(key, value, txnId) {
    if (!this._data.has(key)) this._data.set(key, []);
    const chain = this._data.get(key);
    
    // End the current version
    for (const v of chain) {
      if (v.endTs === Infinity && !v.deleted) {
        v.endTs = txnId;
      }
    }
    
    chain.push({ value, txnId, beginTs: txnId, endTs: Infinity, deleted: false });
  }

  /** Delete (mark as deleted) */
  delete(key, txnId) {
    if (!this._data.has(key)) return false;
    const chain = this._data.get(key);
    for (const v of chain) {
      if (v.endTs === Infinity && !v.deleted) {
        v.endTs = txnId;
      }
    }
    chain.push({ value: null, txnId, beginTs: txnId, endTs: Infinity, deleted: true });
    return true;
  }

  /** Read at a specific timestamp (snapshot read) */
  read(key, readTs) {
    if (!this._data.has(key)) return undefined;
    const chain = this._data.get(key);
    
    // Find the version visible at readTs
    for (let i = chain.length - 1; i >= 0; i--) {
      const v = chain[i];
      if (v.beginTs <= readTs && v.endTs > readTs) {
        return v.deleted ? undefined : v.value;
      }
    }
    return undefined;
  }

  /** Scan all keys visible at timestamp */
  scan(readTs) {
    const results = [];
    for (const [key, chain] of this._data) {
      for (let i = chain.length - 1; i >= 0; i--) {
        const v = chain[i];
        if (v.beginTs <= readTs && v.endTs > readTs && !v.deleted) {
          results.push({ key, value: v.value });
          break;
        }
      }
    }
    return results;
  }

  /** Garbage collect old versions */
  gc(oldestActiveTxn) {
    let collected = 0;
    for (const [key, chain] of this._data) {
      const before = chain.length;
      const filtered = chain.filter(v => v.endTs > oldestActiveTxn || v.beginTs >= oldestActiveTxn);
      if (filtered.length < before) {
        collected += before - filtered.length;
        this._data.set(key, filtered);
      }
      if (filtered.length === 0) this._data.delete(key);
    }
    return collected;
  }

  get keyCount() { return this._data.size; }
  get versionCount() { let c = 0; for (const chain of this._data.values()) c += chain.length; return c; }
}
