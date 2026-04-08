// savepoints.js — Nested transactions with savepoints
// Allows partial rollback within a transaction.
// SAVEPOINT creates a checkpoint. ROLLBACK TO reverts to it.
// RELEASE SAVEPOINT discards it. COMMIT commits everything.

export class SavepointTransaction {
  constructor(txnId) {
    this.txnId = txnId;
    this._operations = []; // [{type, key, oldValue, newValue}]
    this._savepoints = new Map(); // name → operation index
    this._committed = false;
    this._data = new Map(); // Working copy
  }

  read(key) { return this._data.get(key) ?? null; }

  write(key, value) {
    const oldValue = this._data.get(key) ?? null;
    this._operations.push({ type: 'write', key, oldValue, newValue: value });
    this._data.set(key, value);
  }

  delete(key) {
    const oldValue = this._data.get(key) ?? null;
    this._operations.push({ type: 'delete', key, oldValue });
    this._data.delete(key);
  }

  savepoint(name) {
    this._savepoints.set(name, this._operations.length);
  }

  rollbackTo(name) {
    const idx = this._savepoints.get(name);
    if (idx === undefined) throw new Error(`Savepoint '${name}' not found`);

    // Undo operations from current position back to savepoint
    while (this._operations.length > idx) {
      const op = this._operations.pop();
      if (op.type === 'write') {
        if (op.oldValue === null) this._data.delete(op.key);
        else this._data.set(op.key, op.oldValue);
      } else if (op.type === 'delete') {
        if (op.oldValue !== null) this._data.set(op.key, op.oldValue);
      }
    }

    // Remove savepoints after this one
    for (const [sp, spIdx] of this._savepoints) {
      if (spIdx > idx) this._savepoints.delete(sp);
    }
  }

  releaseSavepoint(name) {
    this._savepoints.delete(name);
  }

  commit() {
    this._committed = true;
    return new Map(this._data);
  }

  rollback() {
    // Undo all operations
    while (this._operations.length > 0) {
      const op = this._operations.pop();
      if (op.type === 'write') {
        if (op.oldValue === null) this._data.delete(op.key);
        else this._data.set(op.key, op.oldValue);
      }
    }
    return this._data;
  }

  get operationCount() { return this._operations.length; }
  get savepointNames() { return [...this._savepoints.keys()]; }
}
