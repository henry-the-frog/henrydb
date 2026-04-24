// vector-engine.js — Vectorized execution engine for HenryDB
// Processes batches of rows through operators instead of one-at-a-time.
// Inspired by DuckDB/MonetDB vectorized model.
//
// Key difference from Volcano: nextBatch() returns a VectorBatch (multiple rows)
// instead of next() returning one row. This amortizes per-row overhead.

const BATCH_SIZE = 1024;

/**
 * A batch of rows stored in columnar format.
 * Each column is an array of values. A selection vector
 * tracks which rows are "active" (not filtered out).
 */
export class VectorBatch {
  /**
   * @param {string[]} columns - Column names
   * @param {number} capacity - Max rows per batch
   */
  constructor(columns, capacity = BATCH_SIZE) {
    this.columnNames = columns;
    this.columns = new Map();
    for (const name of columns) {
      this.columns.set(name, new Array(capacity));
    }
    this.size = 0;
    this.capacity = capacity;
  }

  /**
   * Add a row to the batch.
   * @param {object} row - Row object with column values
   * @returns {boolean} true if batch is now full
   */
  addRow(row) {
    const idx = this.size;
    for (const name of this.columnNames) {
      this.columns.get(name)[idx] = row[name] ?? null;
    }
    this.size++;
    return this.size >= this.capacity;
  }

  /**
   * Get a value at (row, column).
   */
  get(rowIdx, colName) {
    return this.columns.get(colName)?.[rowIdx];
  }

  /**
   * Set a value at (row, column).
   */
  set(rowIdx, colName, value) {
    const col = this.columns.get(colName);
    if (col) col[rowIdx] = value;
  }

  /**
   * Convert a row index to a row object.
   */
  getRow(idx) {
    const row = {};
    for (const name of this.columnNames) {
      row[name] = this.columns.get(name)[idx];
    }
    return row;
  }

  /**
   * Convert all rows to row objects.
   * @returns {object[]}
   */
  toRows() {
    const rows = [];
    for (let i = 0; i < this.size; i++) {
      rows.push(this.getRow(i));
    }
    return rows;
  }

  /**
   * Create a new batch with only selected rows.
   * @param {number[]} indices - Row indices to keep
   * @returns {VectorBatch}
   */
  select(indices) {
    const result = new VectorBatch(this.columnNames, indices.length || 1);
    for (const idx of indices) {
      const row = this.getRow(idx);
      result.addRow(row);
    }
    return result;
  }

  /**
   * Add columns from another batch (for joins).
   * @param {VectorBatch} other
   * @param {string} [prefix] - Optional prefix for disambiguation
   * @returns {VectorBatch} New batch with combined columns
   */
  merge(other, prefix) {
    const newCols = [...this.columnNames];
    for (const name of other.columnNames) {
      const qualifiedName = prefix ? `${prefix}.${name}` : name;
      if (!newCols.includes(qualifiedName)) {
        newCols.push(qualifiedName);
      }
    }
    const result = new VectorBatch(newCols, this.size);
    for (let i = 0; i < this.size; i++) {
      const row = this.getRow(i);
      // Add columns from other batch at same index
      if (i < other.size) {
        for (const name of other.columnNames) {
          const qualifiedName = prefix ? `${prefix}.${name}` : name;
          row[qualifiedName] = other.get(i, name);
        }
      }
      result.addRow(row);
    }
    return result;
  }
}

/**
 * Base class for vectorized operators.
 */
class VIterator {
  open() {}
  nextBatch() { return null; }
  close() {}
}

/**
 * Vectorized sequential scan.
 * Reads from a heap file and produces batches.
 */
export class VSeqScan extends VIterator {
  /**
   * @param {object} heap - HeapFile with scan() generator
   * @param {string[]} columns - Column names from schema
   * @param {string} [alias] - Table alias
   */
  constructor(heap, columns, alias) {
    super();
    this._heap = heap;
    this._columns = columns;
    this._alias = alias;
    this._gen = null;
  }

  open() {
    this._gen = this._heap.scan();
  }

  nextBatch() {
    const batch = new VectorBatch(
      this._alias
        ? [...this._columns, ...this._columns.map(c => `${this._alias}.${c}`)]
        : this._columns
    );

    while (batch.size < batch.capacity) {
      const { value, done } = this._gen.next();
      if (done) break;

      const row = {};
      for (let i = 0; i < this._columns.length; i++) {
        row[this._columns[i]] = value.values[i];
        if (this._alias) {
          row[`${this._alias}.${this._columns[i]}`] = value.values[i];
        }
      }
      batch.addRow(row);
    }

    return batch.size > 0 ? batch : null;
  }

  close() {
    this._gen = null;
  }
}

/**
 * Vectorized filter.
 * Evaluates a predicate on each row in the batch.
 */
export class VFilter extends VIterator {
  /**
   * @param {VIterator} child - Input operator
   * @param {function(object): boolean} predicate - Row predicate
   */
  constructor(child, predicate) {
    super();
    this._child = child;
    this._predicate = predicate;
  }

  open() {
    this._child.open();
  }

  nextBatch() {
    while (true) {
      const batch = this._child.nextBatch();
      if (!batch) return null;

      // Evaluate predicate on entire batch → selection vector
      const selected = [];
      for (let i = 0; i < batch.size; i++) {
        const row = batch.getRow(i);
        if (this._predicate(row)) {
          selected.push(i);
        }
      }

      if (selected.length > 0) {
        return batch.select(selected);
      }
      // No rows passed filter, get next batch
    }
  }

  close() {
    this._child.close();
  }
}

/**
 * Vectorized projection.
 * Evaluates expressions and produces new columns.
 */
export class VProject extends VIterator {
  /**
   * @param {VIterator} child
   * @param {Array<{name: string, expr: function(object): any}>} projections
   */
  constructor(child, projections) {
    super();
    this._child = child;
    this._projections = projections;
  }

  open() {
    this._child.open();
  }

  nextBatch() {
    const batch = this._child.nextBatch();
    if (!batch) return null;

    const outCols = this._projections.map(p => p.name);
    const result = new VectorBatch(outCols, batch.size);

    for (let i = 0; i < batch.size; i++) {
      const row = batch.getRow(i);
      const outRow = {};
      for (const proj of this._projections) {
        outRow[proj.name] = proj.expr(row);
      }
      result.addRow(outRow);
    }

    return result;
  }

  close() {
    this._child.close();
  }
}

/**
 * Vectorized hash aggregate.
 * Groups rows by key columns and computes aggregates.
 */
export class VHashAggregate extends VIterator {
  /**
   * @param {VIterator} child
   * @param {string[]} groupCols - Group-by column names
   * @param {Array<{name: string, fn: string, col: string}>} aggregates - e.g. [{name: 'total', fn: 'SUM', col: 'amount'}]
   */
  constructor(child, groupCols, aggregates) {
    super();
    this._child = child;
    this._groupCols = groupCols;
    this._aggregates = aggregates;
    this._result = null;
    this._emitted = false;
  }

  open() {
    this._child.open();
    this._result = null;
    this._emitted = false;

    // Build hash table
    const groups = new Map();

    let batch;
    while ((batch = this._child.nextBatch()) !== null) {
      for (let i = 0; i < batch.size; i++) {
        const row = batch.getRow(i);
        const key = this._groupCols.map(c => row[c]).join('|');

        if (!groups.has(key)) {
          const entry = { key: {}, aggs: {} };
          for (const c of this._groupCols) entry.key[c] = row[c];
          for (const agg of this._aggregates) {
            entry.aggs[agg.name] = { fn: agg.fn, values: [] };
          }
          groups.set(key, entry);
        }

        const entry = groups.get(key);
        for (const agg of this._aggregates) {
          const val = row[agg.col];
          if (val !== null && val !== undefined) {
            entry.aggs[agg.name].values.push(val);
          }
        }
      }
    }

    // Compute aggregates
    const resultRows = [];
    for (const [, entry] of groups) {
      const row = { ...entry.key };
      for (const agg of this._aggregates) {
        const { fn, values } = entry.aggs[agg.name];
        switch (fn) {
          case 'COUNT': row[agg.name] = values.length; break;
          case 'SUM': row[agg.name] = values.length > 0 ? values.reduce((a, b) => a + b, 0) : null; break;
          case 'AVG': row[agg.name] = values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : null; break;
          case 'MIN': row[agg.name] = values.length > 0 ? Math.min(...values) : null; break;
          case 'MAX': row[agg.name] = values.length > 0 ? Math.max(...values) : null; break;
          default: row[agg.name] = null;
        }
      }
      resultRows.push(row);
    }

    this._result = resultRows;
  }

  nextBatch() {
    if (this._emitted || !this._result) return null;
    this._emitted = true;

    const cols = [...this._groupCols, ...this._aggregates.map(a => a.name)];
    const batch = new VectorBatch(cols, this._result.length || 1);
    for (const row of this._result) {
      batch.addRow(row);
    }
    return batch.size > 0 ? batch : null;
  }

  close() {
    this._child.close();
    this._result = null;
  }
}

/**
 * Vectorized hash join.
 * Builds a hash table on the right (build) side, then probes with left (probe) side.
 */
export class VHashJoin extends VIterator {
  /**
   * @param {VIterator} probe - Left/probe side
   * @param {VIterator} build - Right/build side
   * @param {string} probeKey - Probe key column
   * @param {string} buildKey - Build key column
   */
  constructor(probe, build, probeKey, buildKey) {
    super();
    this._probe = probe;
    this._build = build;
    this._probeKey = probeKey;
    this._buildKey = buildKey;
    this._hashTable = null;
    this._buildCols = null;
  }

  open() {
    this._build.open();
    this._probe.open();

    // Build phase: hash all rows from build side
    this._hashTable = new Map();
    let batch;
    while ((batch = this._build.nextBatch()) !== null) {
      if (!this._buildCols) this._buildCols = batch.columnNames;
      for (let i = 0; i < batch.size; i++) {
        const row = batch.getRow(i);
        const key = String(row[this._buildKey]);
        if (!this._hashTable.has(key)) this._hashTable.set(key, []);
        this._hashTable.get(key).push(row);
      }
    }
  }

  nextBatch() {
    while (true) {
      const probeBatch = this._probe.nextBatch();
      if (!probeBatch) return null;

      const allCols = [...new Set([...probeBatch.columnNames, ...(this._buildCols || [])])];
      const result = new VectorBatch(allCols, BATCH_SIZE);

      for (let i = 0; i < probeBatch.size; i++) {
        const probeRow = probeBatch.getRow(i);
        const key = String(probeRow[this._probeKey]);
        const matches = this._hashTable.get(key);
        if (matches) {
          for (const buildRow of matches) {
            const combined = { ...probeRow, ...buildRow };
            result.addRow(combined);
            if (result.size >= result.capacity) break;
          }
        }
      }

      if (result.size > 0) return result;
    }
  }

  close() {
    this._probe.close();
    this._build.close();
    this._hashTable = null;
  }
}
