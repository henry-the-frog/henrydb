// write-batch.js — Batch writer that accumulates writes and flushes atomically
export class WriteBatch {
  constructor() { this._ops = []; }
  put(key, value) { this._ops.push({ type: 'put', key, value }); return this; }
  delete(key) { this._ops.push({ type: 'delete', key }); return this; }
  get size() { return this._ops.length; }
  clear() { this._ops = []; }
  
  apply(store) {
    for (const op of this._ops) {
      if (op.type === 'put') store.set(op.key, op.value);
      else store.delete(op.key);
    }
    this._ops = [];
  }
}
