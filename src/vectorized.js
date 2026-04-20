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

/**
 * VectorizedHashAggregate — GROUP BY with aggregate functions.
 * Consumes all input batches, groups by key columns, and produces
 * aggregate results as a single output batch.
 */
export class VectorizedHashAggregate {
  /**
   * @param {*} child - input operator
   * @param {Array<string>} groupBy - column names to group by (empty = global agg)
   * @param {Array<{name: string, func: string, column: string}>} aggregates
   *   func: 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COUNT_STAR'
   */
  constructor(child, groupBy, aggregates) {
    this._child = child;
    this._groupBy = groupBy;
    this._aggregates = aggregates;
    this._result = null;
  }

  open() {
    this._child.open();
    this._result = null;
  }

  next() {
    if (this._result) return null; // Already returned single result batch

    // Phase 1: Consume all input and build hash table
    const groups = new Map(); // key → { counts, sums, mins, maxs, ... }

    let batch;
    while ((batch = this._child.next()) !== null) {
      const n = batch.activeCount;
      for (let i = 0; i < n; i++) {
        // Compute group key
        let key;
        if (this._groupBy.length === 0) {
          key = '__global__';
        } else if (this._groupBy.length === 1) {
          key = batch.getValue(this._groupBy[0], i);
        } else {
          key = JSON.stringify(this._groupBy.map(c => batch.getValue(c, i)));
        }

        if (!groups.has(key)) {
          const state = {};
          for (const agg of this._aggregates) {
            state[agg.name] = { func: agg.func, sum: 0, count: 0, min: Infinity, max: -Infinity };
          }
          // Store group key values for output
          if (this._groupBy.length > 0) {
            state.__keyValues__ = this._groupBy.map(c => batch.getValue(c, i));
          }
          groups.set(key, state);
        }

        const state = groups.get(key);
        for (const agg of this._aggregates) {
          const s = state[agg.name];
          if (agg.func === 'COUNT_STAR' || agg.func === 'COUNT') {
            if (agg.func === 'COUNT_STAR' || batch.getValue(agg.column, i) != null) {
              s.count++;
            }
          } else {
            const val = batch.getValue(agg.column, i);
            if (val != null) {
              s.count++;
              s.sum += val;
              if (val < s.min) s.min = val;
              if (val > s.max) s.max = val;
            }
          }
        }
      }
    }

    // Handle empty input with global aggregation
    if (groups.size === 0 && this._groupBy.length === 0) {
      groups.set('__global__', (() => {
        const state = {};
        for (const agg of this._aggregates) {
          state[agg.name] = { func: agg.func, sum: 0, count: 0, min: Infinity, max: -Infinity };
        }
        return state;
      })());
    }

    // Phase 2: Build output batch
    const resultColumns = {};
    const colNames = [];

    // Group by columns
    for (const col of this._groupBy) {
      resultColumns[col] = [];
      colNames.push(col);
    }

    // Aggregate columns
    for (const agg of this._aggregates) {
      resultColumns[agg.name] = [];
      colNames.push(agg.name);
    }

    for (const [, state] of groups) {
      // Group by values
      if (state.__keyValues__) {
        for (let g = 0; g < this._groupBy.length; g++) {
          resultColumns[this._groupBy[g]].push(state.__keyValues__[g]);
        }
      }

      // Aggregate values
      for (const agg of this._aggregates) {
        const s = state[agg.name];
        let val;
        switch (agg.func) {
          case 'COUNT':
          case 'COUNT_STAR':
            val = s.count;
            break;
          case 'SUM':
            val = s.count > 0 ? s.sum : null;
            break;
          case 'AVG':
            val = s.count > 0 ? s.sum / s.count : null;
            break;
          case 'MIN':
            val = s.count > 0 ? s.min : null;
            break;
          case 'MAX':
            val = s.count > 0 ? s.max : null;
            break;
          default:
            val = null;
        }
        resultColumns[agg.name].push(val);
      }
    }

    const count = groups.size || (this._groupBy.length === 0 ? 1 : 0);
    this._result = new DataBatch(resultColumns, count);
    return this._result;
  }

  close() {
    this._child.close();
    this._result = null;
  }
}

/**
 * VectorizedHashJoin — hash join with build and probe phases.
 * Build side is fully materialized into a hash table.
 * Probe side streams through in batches.
 */
export class VectorizedHashJoin {
  /**
   * @param {*} buildChild - build side (smaller table)
   * @param {*} probeChild - probe side (larger table)
   * @param {string} buildKey - column name for join key on build side
   * @param {string} probeKey - column name for join key on probe side
   * @param {Array<string>} buildColumns - columns to output from build side
   * @param {Array<string>} probeColumns - columns to output from probe side
   */
  constructor(buildChild, probeChild, buildKey, probeKey, buildColumns, probeColumns) {
    this._buildChild = buildChild;
    this._probeChild = probeChild;
    this._buildKey = buildKey;
    this._probeKey = probeKey;
    this._buildColumns = buildColumns;
    this._probeColumns = probeColumns;
    this._hashTable = null;
  }

  open() {
    this._buildChild.open();
    this._probeChild.open();

    // Build phase: materialize build side into hash table
    this._hashTable = new Map(); // key → [{col: val, ...}]
    let batch;
    while ((batch = this._buildChild.next()) !== null) {
      const n = batch.activeCount;
      for (let i = 0; i < n; i++) {
        const key = batch.getValue(this._buildKey, i);
        const row = {};
        for (const col of this._buildColumns) {
          row[col] = batch.getValue(col, i);
        }
        if (!this._hashTable.has(key)) {
          this._hashTable.set(key, []);
        }
        this._hashTable.get(key).push(row);
      }
    }
    this._buildChild.close();
  }

  next() {
    // Probe phase: for each probe batch, look up matches
    while (true) {
      const probeBatch = this._probeChild.next();
      if (!probeBatch) return null;

      const outputColumns = {};
      const allCols = [...this._probeColumns, ...this._buildColumns.map(c => 'build_' + c)];
      for (const col of allCols) {
        outputColumns[col] = [];
      }

      const n = probeBatch.activeCount;
      let outputCount = 0;

      for (let i = 0; i < n; i++) {
        const key = probeBatch.getValue(this._probeKey, i);
        const matches = this._hashTable.get(key);
        if (!matches) continue;

        for (const buildRow of matches) {
          // Output probe columns
          for (const col of this._probeColumns) {
            outputColumns[col].push(probeBatch.getValue(col, i));
          }
          // Output build columns (prefixed)
          for (const col of this._buildColumns) {
            outputColumns['build_' + col].push(buildRow[col]);
          }
          outputCount++;
        }
      }

      if (outputCount === 0) continue;

      return new DataBatch(outputColumns, outputCount);
    }
  }

  close() {
    this._probeChild.close();
    this._hashTable = null;
  }
}
