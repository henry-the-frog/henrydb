// vectorized.js — Vectorized execution engine for HenryDB
// Processes data in columnar batches (vectors) instead of row-at-a-time.
// Inspired by MonetDB/X100 and DuckDB.

const DEFAULT_BATCH_SIZE = 1024;

/**
 * DataBatch — a columnar chunk of data.
 * Each column is stored as an array. A selection vector tracks active rows.
 */
export class DataBatch {
  /**
   * @param {Object} columns - { colName: Array }
   * @param {number} count - number of rows
   * @param {Int32Array|null} selectionVector - if set, only these indices are active
   */
  constructor(columns, count, selectionVector = null) {
    this.columns = columns;   // { name: Array }
    this.count = count;        // total rows in arrays
    this.sel = selectionVector; // null = all rows active
  }

  /**
   * Number of active rows (respecting selection vector).
   */
  get activeCount() {
    return this.sel ? this.sel.length : this.count;
  }

  /**
   * Get a column value at the given active-row index.
   */
  getValue(colName, activeIdx) {
    const realIdx = this.sel ? this.sel[activeIdx] : activeIdx;
    return this.columns[colName][realIdx];
  }

  /**
   * Materialize to row-oriented format (for result output).
   * Returns array of plain objects.
   */
  toRows(columnNames) {
    const names = columnNames || Object.keys(this.columns);
    const rows = [];
    const n = this.activeCount;
    for (let i = 0; i < n; i++) {
      const row = {};
      for (const name of names) {
        row[name] = this.getValue(name, i);
      }
      rows.push(row);
    }
    return rows;
  }

  /**
   * Create an empty batch with the given column names.
   */
  static empty(columnNames) {
    const columns = {};
    for (const name of columnNames) {
      columns[name] = [];
    }
    return new DataBatch(columns, 0);
  }
}

/**
 * VectorizedScan — reads a HeapFile in batches.
 * Converts row-oriented heap data into columnar batches.
 */
export class VectorizedScan {
  /**
   * @param {HeapFile} heap - the heap to scan
   * @param {Array} schema - column schema [{name, type}]
   * @param {string} tableName - table name for column prefixes
   * @param {number} batchSize - rows per batch
   */
  constructor(heap, schema, tableName, batchSize = DEFAULT_BATCH_SIZE) {
    this._heap = heap;
    this._schema = schema;
    this._tableName = tableName;
    this._batchSize = batchSize;
    this._iterator = null;
    this._done = false;
  }

  /**
   * Initialize the scan.
   */
  open() {
    this._iterator = this._heap.scan();
    this._done = false;
  }

  /**
   * Get next batch of rows. Returns null when exhausted.
   */
  next() {
    if (this._done) return null;
    if (!this._iterator) this.open();

    // Allocate column arrays
    const columns = {};
    const colNames = this._schema.map(c => c.name);
    for (const name of colNames) {
      columns[name] = new Array(this._batchSize);
    }

    let count = 0;
    while (count < this._batchSize) {
      const result = this._iterator.next();
      if (result.done) {
        this._done = true;
        break;
      }
      const { values } = result.value;
      for (let c = 0; c < colNames.length; c++) {
        columns[colNames[c]][count] = values[c];
      }
      count++;
    }

    if (count === 0) return null;

    // Trim arrays to actual count
    if (count < this._batchSize) {
      for (const name of colNames) {
        columns[name].length = count;
      }
    }

    return new DataBatch(columns, count);
  }

  /**
   * Close the scan.
   */
  close() {
    this._iterator = null;
    this._done = true;
  }
}

/**
 * VectorizedFilter — applies a predicate to a batch using selection vectors.
 * Does NOT copy data — just computes which rows pass the filter.
 */
export class VectorizedFilter {
  /**
   * @param {VectorizedScan|VectorizedFilter|VectorizedProject} child - input operator
   * @param {Function} predicate - (batch, activeIdx) => boolean
   */
  constructor(child, predicate) {
    this._child = child;
    this._predicate = predicate;
  }

  open() {
    this._child.open();
  }

  next() {
    while (true) {
      const batch = this._child.next();
      if (!batch) return null;

      // Apply predicate, build selection vector
      const sel = new Int32Array(batch.activeCount);
      let selCount = 0;

      const n = batch.activeCount;
      for (let i = 0; i < n; i++) {
        if (this._predicate(batch, i)) {
          sel[selCount++] = batch.sel ? batch.sel[i] : i;
        }
      }

      if (selCount === 0) continue; // Empty batch after filter, get next

      return new DataBatch(
        batch.columns,
        batch.count,
        sel.subarray(0, selCount)
      );
    }
  }

  close() {
    this._child.close();
  }
}

/**
 * VectorizedProject — computes new columns from existing ones.
 */
export class VectorizedProject {
  /**
   * @param {*} child - input operator
   * @param {Array} projections - [{name, compute: (batch, activeIdx) => value}]
   */
  constructor(child, projections) {
    this._child = child;
    this._projections = projections;
  }

  open() {
    this._child.open();
  }

  next() {
    const batch = this._child.next();
    if (!batch) return null;

    const newColumns = {};
    const n = batch.activeCount;

    for (const proj of this._projections) {
      const col = new Array(n);
      for (let i = 0; i < n; i++) {
        col[i] = proj.compute(batch, i);
      }
      newColumns[proj.name] = col;
    }

    // Project resets selection vector — new columns are dense
    return new DataBatch(newColumns, n);
  }

  close() {
    this._child.close();
  }
}

/**
 * VectorizedLimit — stops after N rows.
 */
export class VectorizedLimit {
  constructor(child, limit) {
    this._child = child;
    this._limit = limit;
    this._seen = 0;
  }

  open() {
    this._child.open();
    this._seen = 0;
  }

  next() {
    if (this._seen >= this._limit) return null;

    const batch = this._child.next();
    if (!batch) return null;

    const remaining = this._limit - this._seen;
    const take = Math.min(batch.activeCount, remaining);
    this._seen += take;

    if (take === batch.activeCount) return batch;

    // Need to truncate
    if (batch.sel) {
      return new DataBatch(batch.columns, batch.count, batch.sel.subarray(0, take));
    }
    // No sel — create one
    const sel = new Int32Array(take);
    for (let i = 0; i < take; i++) sel[i] = i;
    return new DataBatch(batch.columns, batch.count, sel);
  }

  close() {
    this._child.close();
  }
}

/**
 * Collect all rows from a vectorized operator into a flat array.
 */
export function collectAll(operator, columnNames) {
  operator.open();
  const allRows = [];
  let batch;
  while ((batch = operator.next()) !== null) {
    allRows.push(...batch.toRows(columnNames));
  }
  operator.close();
  return allRows;
}
