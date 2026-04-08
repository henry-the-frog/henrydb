// wal-replay.js — Write-Ahead Log with replay for crash recovery
// Records operations as log entries, replays them to reconstruct state.

export class WALEntry {
  constructor(lsn, txnId, type, table, key, before, after) {
    this.lsn = lsn;
    this.txnId = txnId;
    this.type = type; // 'INSERT' | 'UPDATE' | 'DELETE' | 'COMMIT' | 'ABORT' | 'CHECKPOINT'
    this.table = table;
    this.key = key;
    this.before = before;
    this.after = after;
    this.timestamp = Date.now();
  }
}

export class WAL {
  constructor() {
    this._log = [];
    this._lsn = 0;
    this._committed = new Set();
    this._aborted = new Set();
  }

  /** Append a log entry */
  append(txnId, type, table = null, key = null, before = null, after = null) {
    const entry = new WALEntry(++this._lsn, txnId, type, table, key, before, after);
    this._log.push(entry);
    if (type === 'COMMIT') this._committed.add(txnId);
    if (type === 'ABORT') this._aborted.add(txnId);
    return entry;
  }

  /** Replay log to reconstruct state (redo committed, skip aborted) */
  replay() {
    const state = new Map(); // table → Map<key, value>
    
    for (const entry of this._log) {
      if (entry.type === 'COMMIT' || entry.type === 'ABORT' || entry.type === 'CHECKPOINT') continue;
      if (this._aborted.has(entry.txnId)) continue;
      if (!this._committed.has(entry.txnId)) continue;
      
      if (!state.has(entry.table)) state.set(entry.table, new Map());
      const table = state.get(entry.table);
      
      switch (entry.type) {
        case 'INSERT': case 'UPDATE':
          table.set(entry.key, entry.after);
          break;
        case 'DELETE':
          table.delete(entry.key);
          break;
      }
    }
    return state;
  }

  /** Undo uncommitted transactions */
  undo() {
    const undone = [];
    for (let i = this._log.length - 1; i >= 0; i--) {
      const entry = this._log[i];
      if (entry.type === 'COMMIT' || entry.type === 'ABORT' || entry.type === 'CHECKPOINT') continue;
      if (!this._committed.has(entry.txnId) && !this._aborted.has(entry.txnId)) {
        undone.push({ txnId: entry.txnId, type: entry.type, table: entry.table, key: entry.key, before: entry.before });
      }
    }
    return undone;
  }

  /** Truncate log up to LSN (after checkpoint) */
  truncate(lsn) {
    this._log = this._log.filter(e => e.lsn > lsn);
  }

  /** Create checkpoint */
  checkpoint() {
    return this.append(0, 'CHECKPOINT');
  }

  get length() { return this._log.length; }
  get currentLSN() { return this._lsn; }
  get committedCount() { return this._committed.size; }
}
