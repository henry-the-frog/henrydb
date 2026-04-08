// write-batch.js — Atomic multi-key writes with rollback
// All writes in a batch either all succeed or all roll back.

export class WriteBatch {
  constructor(store) {
    this.store = store;
    this._ops = [];
    this._committed = false;
  }

  put(key, value) { this._ops.push({ type: 'PUT', key, value }); return this; }
  delete(key) { this._ops.push({ type: 'DELETE', key }); return this; }

  /** Commit all operations atomically */
  commit() {
    if (this._committed) throw new Error('Batch already committed');
    const undoLog = [];
    
    try {
      for (const op of this._ops) {
        const oldValue = this.store.get(op.key);
        undoLog.push({ key: op.key, oldValue, existed: oldValue !== undefined });
        
        if (op.type === 'PUT') this.store.set(op.key, op.value);
        else if (op.type === 'DELETE') this.store.delete(op.key);
      }
      this._committed = true;
      return { ok: true, operations: this._ops.length };
    } catch (err) {
      // Rollback
      for (let i = undoLog.length - 1; i >= 0; i--) {
        const undo = undoLog[i];
        if (undo.existed) this.store.set(undo.key, undo.oldValue);
        else this.store.delete(undo.key);
      }
      return { ok: false, error: err.message };
    }
  }

  get size() { return this._ops.length; }
}

/**
 * Tournament Tree (Loser Tree) — efficient k-way merge.
 */
export class TournamentTree {
  constructor(sources) {
    this.k = sources.length;
    this.sources = sources.map(s => ({ iter: s[Symbol.iterator](), current: null, done: false }));
    this._tree = new Array(this.k).fill(-1);
    this._init();
  }

  _init() {
    for (let i = 0; i < this.k; i++) {
      const next = this.sources[i].iter.next();
      this.sources[i].current = next.done ? null : next.value;
      this.sources[i].done = next.done;
    }
  }

  /** Get all elements in sorted order */
  *merge() {
    while (true) {
      // Find minimum among current elements
      let minIdx = -1;
      let minVal = Infinity;
      for (let i = 0; i < this.k; i++) {
        if (!this.sources[i].done && this.sources[i].current !== null) {
          const val = typeof this.sources[i].current === 'object' ? this.sources[i].current.key : this.sources[i].current;
          if (val < minVal) { minVal = val; minIdx = i; }
        }
      }
      
      if (minIdx === -1) break;
      
      yield this.sources[minIdx].current;
      
      const next = this.sources[minIdx].iter.next();
      if (next.done) {
        this.sources[minIdx].done = true;
        this.sources[minIdx].current = null;
      } else {
        this.sources[minIdx].current = next.value;
      }
    }
  }
}

/**
 * Page Layout — NSM (N-ary Storage Model) vs DSM (Decomposition Storage Model).
 */
export class NSMPage {
  constructor(pageSize = 4096) {
    this.pageSize = pageSize;
    this._rows = [];
    this._usedBytes = 0;
  }

  insert(row) {
    const size = JSON.stringify(row).length;
    if (this._usedBytes + size > this.pageSize) return false;
    this._rows.push(row);
    this._usedBytes += size;
    return true;
  }

  get(idx) { return this._rows[idx]; }
  scan() { return [...this._rows]; }
  get count() { return this._rows.length; }
  get utilization() { return this._usedBytes / this.pageSize; }
}

export class DSMPage {
  constructor(columns, pageSize = 4096) {
    this.pageSize = pageSize;
    this.columns = columns;
    this._data = {};
    for (const col of columns) this._data[col] = [];
    this._count = 0;
  }

  insert(row) {
    for (const col of this.columns) this._data[col].push(row[col]);
    this._count++;
    return true;
  }

  getColumn(col) { return this._data[col]; }
  
  reconstruct(idx) {
    const row = {};
    for (const col of this.columns) row[col] = this._data[col][idx];
    return row;
  }

  get count() { return this._count; }
}
