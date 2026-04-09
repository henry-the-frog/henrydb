// tuple.js — Immutable typed tuple for row storage
export class Tuple {
  constructor(schema, values) {
    this._schema = schema;
    this._values = Object.freeze([...values]);
    Object.freeze(this);
  }

  get(name) {
    const idx = this._schema.indexOf(name);
    return idx >= 0 ? this._values[idx] : undefined;
  }

  getByIndex(idx) { return this._values[idx]; }
  get length() { return this._values.length; }
  get schema() { return this._schema; }
  
  toObject() {
    const obj = {};
    for (let i = 0; i < this._schema.length; i++) obj[this._schema[i]] = this._values[i];
    return obj;
  }

  equals(other) {
    if (this._values.length !== other._values.length) return false;
    for (let i = 0; i < this._values.length; i++) {
      if (this._values[i] !== other._values[i]) return false;
    }
    return true;
  }

  project(columns) {
    const indices = columns.map(c => this._schema.indexOf(c));
    return new Tuple(columns, indices.map(i => this._values[i]));
  }
}
