// column-store.js — Columnar storage engine for HenryDB
// Optimized for analytical queries (aggregations over many rows, few columns).

/**
 * Column Store: stores data column-by-column for fast analytics.
 */
export class ColumnStore {
  constructor(schema) {
    // schema: [{ name, type }]
    this._schema = schema;
    this._columns = new Map();
    for (const col of schema) {
      this._columns.set(col.name, []);
    }
    this._rowCount = 0;
  }

  /**
   * Insert a row (object with column values).
   */
  insert(row) {
    for (const col of this._schema) {
      this._columns.get(col.name).push(row[col.name] ?? null);
    }
    this._rowCount++;
  }

  /**
   * Bulk insert rows.
   */
  insertBatch(rows) {
    for (const row of rows) this.insert(row);
  }

  /**
   * Get a column as an array.
   */
  getColumn(name) {
    return this._columns.get(name);
  }

  /**
   * Scan specific columns only (projection pushdown).
   */
  scan(columns, predicate = null) {
    const result = [];
    for (let i = 0; i < this._rowCount; i++) {
      if (predicate) {
        const row = {};
        for (const col of this._schema) {
          row[col.name] = this._columns.get(col.name)[i];
        }
        if (!predicate(row)) continue;
      }
      const out = {};
      for (const col of columns) {
        out[col] = this._columns.get(col)[i];
      }
      result.push(out);
    }
    return result;
  }

  /**
   * Aggregate a column (vectorized).
   */
  aggregate(column, func) {
    const data = this._columns.get(column);
    if (!data) throw new Error(`Column ${column} not found`);

    switch (func) {
      case 'sum': return data.reduce((a, b) => a + (b ?? 0), 0);
      case 'avg': {
        const nonNull = data.filter(v => v != null);
        return nonNull.length ? nonNull.reduce((a, b) => a + b, 0) / nonNull.length : null;
      }
      case 'min': {
        const nonNull = data.filter(v => v != null);
        return nonNull.length ? Math.min(...nonNull) : null;
      }
      case 'max': {
        const nonNull = data.filter(v => v != null);
        return nonNull.length ? Math.max(...nonNull) : null;
      }
      case 'count': return data.filter(v => v != null).length;
      case 'count_all': return data.length;
      default: throw new Error(`Unknown aggregate: ${func}`);
    }
  }

  /**
   * Group-by aggregation (hash aggregation).
   */
  groupBy(groupColumn, aggColumn, aggFunc) {
    const groups = new Map();
    const groupData = this._columns.get(groupColumn);
    const aggData = this._columns.get(aggColumn);
    
    for (let i = 0; i < this._rowCount; i++) {
      const key = groupData[i];
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(aggData[i]);
    }
    
    const result = [];
    for (const [key, values] of groups) {
      let value;
      const nonNull = values.filter(v => v != null);
      switch (aggFunc) {
        case 'sum': value = nonNull.reduce((a, b) => a + b, 0); break;
        case 'avg': value = nonNull.length ? nonNull.reduce((a, b) => a + b, 0) / nonNull.length : null; break;
        case 'min': value = nonNull.length ? Math.min(...nonNull) : null; break;
        case 'max': value = nonNull.length ? Math.max(...nonNull) : null; break;
        case 'count': value = nonNull.length; break;
        default: throw new Error(`Unknown aggregate: ${aggFunc}`);
      }
      result.push({ [groupColumn]: key, [aggFunc]: value, count: values.length });
    }
    return result;
  }

  /**
   * Run-length encode a column (compression).
   */
  rleEncode(column) {
    const data = this._columns.get(column);
    if (!data || data.length === 0) return [];
    
    const encoded = [];
    let current = data[0];
    let count = 1;
    
    for (let i = 1; i < data.length; i++) {
      if (data[i] === current) {
        count++;
      } else {
        encoded.push({ value: current, count });
        current = data[i];
        count = 1;
      }
    }
    encoded.push({ value: current, count });
    return encoded;
  }

  /**
   * Dictionary encode a column (compression for low-cardinality).
   */
  dictEncode(column) {
    const data = this._columns.get(column);
    const dict = new Map();
    const codes = [];
    let nextCode = 0;
    
    for (const val of data) {
      if (!dict.has(val)) {
        dict.set(val, nextCode++);
      }
      codes.push(dict.get(val));
    }
    
    return {
      dictionary: [...dict.entries()].map(([val, code]) => ({ code, value: val })),
      codes,
      compressionRatio: data.length > 0 ? dict.size / data.length : 0,
    };
  }

  get rowCount() { return this._rowCount; }
  get columnCount() { return this._schema.length; }
}
