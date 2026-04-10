// recovery.js — Crash recovery using WAL (simplified ARIES)
import { LogTypes } from './log-record.js';

export class RecoveryManager {
  constructor(logManager) { this._log = logManager; }

  /** REDO phase: replay committed transactions */
  redo(store) {
    const committed = new Set();
    for (const rec of this._log.getAll()) {
      if (rec.type === LogTypes.COMMIT) committed.add(rec.txId);
    }
    let redone = 0;
    for (const rec of this._log.getAll()) {
      if (rec.type === LogTypes.UPDATE && committed.has(rec.txId) && rec.after !== undefined) {
        store.set(rec.pageId, rec.after);
        redone++;
      }
    }
    return redone;
  }

  /** UNDO phase: rollback uncommitted transactions */
  undo(store) {
    const committed = new Set();
    for (const rec of this._log.getAll()) {
      if (rec.type === LogTypes.COMMIT) committed.add(rec.txId);
    }
    let undone = 0;
    for (let i = this._log.getAll().length - 1; i >= 0; i--) {
      const rec = this._log.getAll()[i];
      if (rec.type === LogTypes.UPDATE && !committed.has(rec.txId) && rec.before !== undefined) {
        store.set(rec.pageId, rec.before);
        undone++;
      }
    }
    return undone;
  }
}
