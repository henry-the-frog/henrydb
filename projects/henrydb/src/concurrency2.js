// concurrency2.js — Intent locks, gap locks, RCU, WAL group commit

/**
 * Intent Locks — hierarchical locking for multi-granularity concurrency.
 * Lock modes: IS (intent shared), IX (intent exclusive), S (shared), X (exclusive), SIX
 */
export class IntentLockManager {
  constructor() {
    this._locks = new Map(); // resource → Map<txnId, mode>
  }

  // Compatibility matrix
  static COMPAT = {
    IS:  { IS: true,  IX: true,  S: true,  SIX: true,  X: false },
    IX:  { IS: true,  IX: true,  S: false, SIX: false, X: false },
    S:   { IS: true,  IX: false, S: true,  SIX: false, X: false },
    SIX: { IS: true,  IX: false, S: false, SIX: false, X: false },
    X:   { IS: false, IX: false, S: false, SIX: false, X: false },
  };

  lock(resource, txnId, mode) {
    if (!this._locks.has(resource)) this._locks.set(resource, new Map());
    const holders = this._locks.get(resource);
    
    // Check compatibility with existing locks
    for (const [holderId, holdMode] of holders) {
      if (holderId === txnId) continue;
      if (!IntentLockManager.COMPAT[mode]?.[holdMode]) {
        return { granted: false, conflictWith: holderId, conflictMode: holdMode };
      }
    }
    
    holders.set(txnId, mode);
    return { granted: true };
  }

  unlock(resource, txnId) {
    const holders = this._locks.get(resource);
    if (holders) {
      holders.delete(txnId);
      if (holders.size === 0) this._locks.delete(resource);
    }
  }

  unlockAll(txnId) {
    for (const [resource, holders] of this._locks) {
      holders.delete(txnId);
      if (holders.size === 0) this._locks.delete(resource);
    }
  }

  getHolders(resource) {
    return this._locks.has(resource) ? [...this._locks.get(resource).entries()] : [];
  }
}

/**
 * Gap Lock — range locking to prevent phantom reads.
 */
export class GapLockManager {
  constructor() {
    this._gaps = []; // [{txnId, lo, hi, mode}]
  }

  lockGap(txnId, lo, hi, mode = 'S') {
    // Check for conflicts
    for (const gap of this._gaps) {
      if (gap.txnId === txnId) continue;
      if (this._overlaps(lo, hi, gap.lo, gap.hi)) {
        if (mode === 'X' || gap.mode === 'X') {
          return { granted: false, conflictWith: gap.txnId };
        }
      }
    }
    this._gaps.push({ txnId, lo, hi, mode });
    return { granted: true };
  }

  unlockAll(txnId) {
    this._gaps = this._gaps.filter(g => g.txnId !== txnId);
  }

  _overlaps(lo1, hi1, lo2, hi2) {
    return lo1 <= hi2 && lo2 <= hi1;
  }

  get lockCount() { return this._gaps.length; }
}

/**
 * RCU (Read-Copy-Update) — lockless concurrent reads.
 */
export class RCU {
  constructor() {
    this._data = null;
    this._version = 0;
    this._readers = new Map(); // readerId → version
  }

  /** Publish new data (writer side) */
  publish(data) {
    this._data = data;
    this._version++;
    return this._version;
  }

  /** Start a read-side critical section */
  readLock(readerId) {
    this._readers.set(readerId, this._version);
    return this._data;
  }

  /** End a read-side critical section */
  readUnlock(readerId) {
    this._readers.delete(readerId);
  }

  /** Wait for all readers to finish (synchronize_rcu) */
  synchronize() {
    // In real impl this would wait; here we just check
    return this._readers.size === 0;
  }

  /** Check if a version can be reclaimed */
  canReclaim(version) {
    for (const readVersion of this._readers.values()) {
      if (readVersion <= version) return false;
    }
    return true;
  }

  get version() { return this._version; }
  get readerCount() { return this._readers.size; }
}

/**
 * WAL Group Commit — batch fsync for higher throughput.
 */
export class GroupCommitWAL {
  constructor(flushIntervalMs = 10) {
    this._buffer = [];
    this._flushInterval = flushIntervalMs;
    this._lsn = 0;
    this._flushedLSN = 0;
    this._flushCount = 0;
    this._totalRecords = 0;
  }

  /** Append a log record (returns LSN) */
  append(record) {
    this._lsn++;
    this._buffer.push({ lsn: this._lsn, ...record });
    this._totalRecords++;
    return this._lsn;
  }

  /** Flush buffered records (simulates fsync) */
  flush() {
    if (this._buffer.length === 0) return { flushed: 0 };
    const count = this._buffer.length;
    this._flushedLSN = this._buffer[this._buffer.length - 1].lsn;
    this._buffer = [];
    this._flushCount++;
    return { flushed: count, flushedLSN: this._flushedLSN };
  }

  /** Check if LSN is durable */
  isDurable(lsn) { return lsn <= this._flushedLSN; }

  get pendingCount() { return this._buffer.length; }
  get stats() { return { totalRecords: this._totalRecords, flushCount: this._flushCount, avgBatchSize: this._flushCount > 0 ? this._totalRecords / this._flushCount : 0 }; }
}
