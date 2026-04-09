// access-method.js — Unified access method interface
// All index types implement the same interface for the query executor.
export class AccessMethod {
  constructor(name, type) { this._name = name; this._type = type; this._data = new Map(); }
  
  get name() { return this._name; }
  get type() { return this._type; }

  insert(key, rowId) { this._data.set(key, rowId); }
  lookup(key) { return this._data.get(key); }
  
  scan() { return [...this._data.entries()].map(([key, rowId]) => ({ key, rowId })); }
  
  rangeScan(lo, hi) {
    return this.scan().filter(e => e.key >= lo && e.key <= hi).sort((a, b) => a.key - b.key);
  }

  get size() { return this._data.size; }
}

export class AccessMethodRegistry {
  constructor() { this._methods = new Map(); }
  register(name, method) { this._methods.set(name, method); }
  get(name) { return this._methods.get(name); }
  list() { return [...this._methods.keys()]; }
}
