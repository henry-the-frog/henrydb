// recovery.js — Crash recovery using WAL (simplified ARIES)
export class RecoveryManager {
  constructor(logManager) { this._log = logManager; }

  /** REDO phase: replay committed transactions */
  redo(store) {
    const committed = new Set();
    for (const rec of this._log.getAll()) {
      if (rec.type === 5) committed.add(rec.txId); // COMMIT
    }
    let redone = 0;
    for (const rec of this._log.getAll()) {
      if (rec.type === 2 && committed.has(rec.txId) && rec.after !== undefined) {
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
      if (rec.type === 5) committed.add(rec.txId);
    }
    let undone = 0;
    for (let i = this._log.getAll().length - 1; i >= 0; i--) {
      const rec = this._log.getAll()[i];
      if (rec.type === 2 && !committed.has(rec.txId) && rec.before !== undefined) {
        store.set(rec.pageId, rec.before);
        undone++;
      }
    }
    return undone;
  }
}
