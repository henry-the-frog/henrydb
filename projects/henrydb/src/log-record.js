// log-record.js — Structured log records for ARIES recovery
export const LogTypes = { BEGIN: 1, UPDATE: 2, COMMIT: 3, ABORT: 4, UNDO: 5, CLR: 6, CHECKPOINT: 7, END: 8 };

export class LogRecord {
  constructor(lsn, type, txId, pageId, before, after) {
    this.lsn = lsn;
    this.type = type;
    this.txId = txId;
    this.pageId = pageId;
    this.before = before; // For UNDO
    this.after = after;   // For REDO
    this.prevLSN = null;
  }
}

export class LogManager {
  constructor() { this._log = []; this._nextLSN = 1; }
  
  write(type, txId, pageId, before, after) {
    const record = new LogRecord(this._nextLSN++, type, txId, pageId, before, after);
    this._log.push(record);
    return record.lsn;
  }

  get(lsn) { return this._log.find(r => r.lsn === lsn); }
  getAll() { return [...this._log]; }
  get size() { return this._log.length; }
  
  /** Get all records for a transaction (for rollback). */
  getByTx(txId) { return this._log.filter(r => r.txId === txId); }
}
