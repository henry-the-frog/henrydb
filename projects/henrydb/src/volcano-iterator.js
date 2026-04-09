// volcano-iterator.js — Volcano/Iterator model for query execution
// Each operator is an iterator with open/next/close. Pull-based execution.
// Used in virtually every RDBMS (PostgreSQL, MySQL, SQLite).

export class SeqScan {
  constructor(data) { this._data = data; this._pos = 0; }
  open() { this._pos = 0; }
  next() { return this._pos < this._data.length ? this._data[this._pos++] : null; }
  close() {}
}

export class Filter {
  constructor(child, predicate) { this._child = child; this._predicate = predicate; }
  open() { this._child.open(); }
  next() {
    let row;
    while ((row = this._child.next()) !== null) {
      if (this._predicate(row)) return row;
    }
    return null;
  }
  close() { this._child.close(); }
}

export class Project {
  constructor(child, columns) { this._child = child; this._columns = columns; }
  open() { this._child.open(); }
  next() {
    const row = this._child.next();
    if (!row) return null;
    const result = {};
    for (const col of this._columns) result[col] = row[col];
    return result;
  }
  close() { this._child.close(); }
}

export class Limit {
  constructor(child, n) { this._child = child; this._limit = n; this._count = 0; }
  open() { this._child.open(); this._count = 0; }
  next() {
    if (this._count >= this._limit) return null;
    this._count++;
    return this._child.next();
  }
  close() { this._child.close(); }
}

/** Collect all rows from an iterator into an array. */
export function collect(iter) {
  iter.open();
  const rows = [];
  let row;
  while ((row = iter.next()) !== null) rows.push(row);
  iter.close();
  return rows;
}
