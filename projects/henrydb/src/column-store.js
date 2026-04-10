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

  // ============================================================
  // Object-based API (key-value insert)
  // ============================================================

  /** Insert a row from an object {colName: value}. */
  insert(obj) {
    for (const col of this._schema) {
      this._columns.get(col.name).push(obj[col.name] ?? null);
    }
    this._size++;
  }

  /** Scan all rows, yielding objects. */
  *scan() {
    for (let i = 0; i < this._size; i++) {
      const row = {};
      for (const col of this._schema) {
        row[col.name] = this._columns.get(col.name)[i];
      }
      yield row;
    }
  }

  // ============================================================
  // Dictionary Encoding
  // ============================================================

  /**
   * Auto-detect low-cardinality TEXT columns and dictionary-encode them.
   * Returns {encoded: [{column, cardinality, compressionRatio}]}
   */
  autoDictEncode(cardinalityThreshold = 0.1) {
    const encoded = [];
    for (const col of this._schema) {
      if (col.type !== 'TEXT') continue;
      const data = this._columns.get(col.name);
      if (data.length === 0) continue;
      
      const unique = new Set(data);
      const cardinality = unique.size;
      const ratio = cardinality / data.length;
      
      if (ratio <= cardinalityThreshold) {
        // Build dictionary
        const dict = [...unique];
        const dictMap = new Map(dict.map((v, i) => [v, i]));
        const codes = new Uint16Array(data.length);
        for (let i = 0; i < data.length; i++) {
          codes[i] = dictMap.get(data[i]);
        }
        
        // Store encoded version
        if (!this._dictionaries) this._dictionaries = new Map();
        this._dictionaries.set(col.name, { dict, dictMap, codes });
        
        const compressionRatio = data.length > 0 
          ? (data.reduce((s, v) => s + (v ? v.length : 0), 0)) / (codes.byteLength + dict.join('').length)
          : 1;
        
        encoded.push({ column: col.name, cardinality, compressionRatio: Math.round(compressionRatio * 10) / 10 });
      }
    }
    return { encoded };
  }

  /**
   * Filter using dictionary encoding (fast equality filter on encoded column).
   * Returns row indices matching the value.
   */
  dictFilter(colName, value) {
    if (this._dictionaries && this._dictionaries.has(colName)) {
      const { dictMap, codes } = this._dictionaries.get(colName);
      const code = dictMap.get(value);
      if (code === undefined) return [];
      const indices = [];
      for (let i = 0; i < codes.length; i++) {
        if (codes[i] === code) indices.push(i);
      }
      return indices;
    }
    // Fallback: scan raw column
    return this.filter(colName, v => v === value);
  }

  /**
   * Get dictionary for a column (if encoded).
   */
  getDictionary(colName) {
    return this._dictionaries?.get(colName) || null;
  }

  /** Alias for getDictionary */
  getDictColumn(colName) {
    return this._dictionaries?.get(colName) || null;
  }

  /**
   * Group by a dictionary-encoded column.
   * Returns Map<groupKey, array of row indices>.
   * Much faster than scanning raw data for low-cardinality columns.
   */
  dictGroupBy(colName) {
    const dictEntry = this._dictionaries?.get(colName);
    if (dictEntry) {
      // Fast path: use encoded codes
      const { dict, codes } = dictEntry;
      const groups = new Map();
      for (const val of dict) groups.set(val, []);
      for (let i = 0; i < codes.length; i++) {
        groups.get(dict[codes[i]]).push(i);
      }
      return groups;
    }
    // Fallback: scan raw column
    const col = this._columns.get(colName);
    const groups = new Map();
    for (let i = 0; i < col.length; i++) {
      const key = col[i];
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(i);
    }
    return groups;
  }
}
