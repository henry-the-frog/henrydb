// cursor.js — Database cursor for iterating over result sets
export class Cursor {
  constructor(data) {
    this._data = data;
    this._pos = -1;
    this._closed = false;
  }

  next() {
    if (this._closed) return null;
    this._pos++;
    return this._pos < this._data.length ? this._data[this._pos] : null;
  }

  peek() { return this._pos + 1 < this._data.length ? this._data[this._pos + 1] : null; }
  current() { return this._pos >= 0 && this._pos < this._data.length ? this._data[this._pos] : null; }
  reset() { this._pos = -1; }
  close() { this._closed = true; }
  get position() { return this._pos; }
  get isClosed() { return this._closed; }

  fetch(n) {
    const rows = [];
    for (let i = 0; i < n; i++) {
      const row = this.next();
      if (!row) break;
      rows.push(row);
    }
    return rows;
  }

  *[Symbol.iterator]() {
    let row;
    while ((row = this.next()) !== null) yield row;
  }
}
