// snapshot.js — Immutable database snapshot for consistent reads
export class Snapshot {
  constructor(data) {
    this._data = new Map(data); // Copy at snapshot time
    this._timestamp = Date.now();
    Object.freeze(this);
  }

  get(key) { return this._data.get(key); }
  has(key) { return this._data.has(key); }
  get size() { return this._data.size; }
  get timestamp() { return this._timestamp; }
  
  *[Symbol.iterator]() { yield* this._data; }
}

export class SnapshotStore {
  constructor() {
    this._data = new Map();
    this._snapshots = [];
  }

  set(key, value) { this._data.set(key, value); }
  get(key) { return this._data.get(key); }
  delete(key) { this._data.delete(key); }

  snapshot() {
    const snap = new Snapshot(this._data);
    this._snapshots.push(snap);
    return snap;
  }

  get snapshotCount() { return this._snapshots.length; }
}
