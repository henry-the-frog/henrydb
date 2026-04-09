// column-store.js — Columnar storage layout for analytics
// Stores each column as a separate typed array.
// Used in: DuckDB, ClickHouse, Vertica, Amazon Redshift.

export class ColumnStore {
  constructor(schema) {
    this._schema = schema; // [{name, type}]
    this._columns = new Map();
    this._size = 0;
    for (const col of schema) {
      this._columns.set(col.name, []);
    }
  }

  get rowCount() { return this._size; }
  get columnNames() { return this._schema.map(s => s.name); }

  /** Append a row. */
  appendRow(values) {
    for (let i = 0; i < this._schema.length; i++) {
      this._columns.get(this._schema[i].name).push(values[i]);
    }
    this._size++;
  }

  /** Bulk append rows. */
  appendBatch(rows) {
    for (const row of rows) this.appendRow(row);
  }

  /** Get entire column as array (vectorized access). */
  getColumn(name) { return this._columns.get(name); }

  /** Column-wise SUM. */
  sum(name) {
    return this._columns.get(name).reduce((a, b) => a + b, 0);
  }

  /** Column-wise AVG. */
  avg(name) {
    const col = this._columns.get(name);
    return col.reduce((a, b) => a + b, 0) / col.length;
  }

  /** Column-wise MIN/MAX. */
  min(name) { return Math.min(...this._columns.get(name)); }
  max(name) { return Math.max(...this._columns.get(name)); }

  /** Filter: return row indices where predicate is true. */
  filter(name, predicate) {
    const col = this._columns.get(name);
    const indices = [];
    for (let i = 0; i < col.length; i++) {
      if (predicate(col[i])) indices.push(i);
    }
    return indices;
  }

  /** Project: get selected columns for given row indices. */
  project(columns, indices) {
    return indices.map(i => {
      const row = {};
      for (const col of columns) row[col] = this._columns.get(col)[i];
      return row;
    });
  }

  /** Group by a column and aggregate. */
  groupBy(groupCol, aggCol, aggFn = 'sum') {
    const groups = new Map();
    const gcol = this._columns.get(groupCol);
    const acol = this._columns.get(aggCol);
    
    for (let i = 0; i < this._size; i++) {
      const key = gcol[i];
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(acol[i]);
    }
    
    const result = [];
    for (const [key, values] of groups) {
      let agg;
      if (aggFn === 'sum') agg = values.reduce((a, b) => a + b, 0);
      else if (aggFn === 'avg') agg = values.reduce((a, b) => a + b, 0) / values.length;
      else if (aggFn === 'count') agg = values.length;
      else if (aggFn === 'min') agg = Math.min(...values);
      else if (aggFn === 'max') agg = Math.max(...values);
      result.push({ [groupCol]: key, [aggFn]: agg });
    }
    return result;
  }
}
