// vectorized.js — Vectorized execution engine for HenryDB
// Instead of processing one row at a time, processes batches of column values.
// Key advantage: better CPU cache utilization, SIMD-friendly data layout,
// reduced per-row interpretation overhead.

const DEFAULT_BATCH_SIZE = 1024;

/**
 * ColumnBatch — a batch of columnar data.
 * Each column is stored as a flat array (or typed array for numbers).
 * A selection vector tracks which rows are "active" (pass filters).
 */
export class ColumnBatch {
  /**
   * @param {Map<string, any[]>} columns — column name → value array
   * @param {number} length — number of rows
   * @param {Uint32Array|null} selection — active row indices (null = all active)
   */
  constructor(columns, length, selection = null) {
    this.columns = columns;     // Map<string, any[]>
    this.length = length;       // total rows in batch
    this.selection = selection;  // null means all rows active
    this._activeCount = selection ? selection.length : length;
  }

  /** Number of active (non-filtered) rows. */
  get activeCount() { return this._activeCount; }

  /** Get a column's values array. */
  getColumn(name) { return this.columns.get(name); }

  /** Get the value at a specific row for a column. */
  getValue(col, rowIdx) {
    const arr = this.columns.get(col);
    return arr ? arr[rowIdx] : undefined;
  }

  /** Create a new batch with a subset of active rows. */
  filter(newSelection) {
    return new ColumnBatch(this.columns, this.length, newSelection);
  }

  /** Iterate active rows as objects (for final output). */
  *rows() {
    const colNames = [...this.columns.keys()];
    const colArrays = colNames.map(n => this.columns.get(n));
    
    if (this.selection) {
      for (const idx of this.selection) {
        const row = {};
        for (let c = 0; c < colNames.length; c++) {
          row[colNames[c]] = colArrays[c][idx];
        }
        yield row;
      }
    } else {
      for (let i = 0; i < this.length; i++) {
        const row = {};
        for (let c = 0; c < colNames.length; c++) {
          row[colNames[c]] = colArrays[c][i];
        }
        yield row;
      }
    }
  }
}

/**
 * VectorizedScan — read a heap into column batches.
 */
export class VectorizedScan {
  constructor(heap, schema, batchSize = DEFAULT_BATCH_SIZE) {
    this._heap = heap;
    this._schema = schema;
    this._batchSize = batchSize;
  }

  /** Yield column batches from the heap. */
  *execute() {
    const colNames = this._schema.map(s => s.name);
    let batch = new Map();
    for (const name of colNames) batch.set(name, []);
    let count = 0;

    for (const entry of this._heap.scan()) {
      for (let i = 0; i < colNames.length; i++) {
        batch.get(colNames[i]).push(entry.values[i]);
      }
      count++;

      if (count >= this._batchSize) {
        yield new ColumnBatch(batch, count);
        batch = new Map();
        for (const name of colNames) batch.set(name, []);
        count = 0;
      }
    }

    if (count > 0) {
      yield new ColumnBatch(batch, count);
    }
  }
}

/**
 * VectorizedFilter — apply a filter predicate to an entire batch at once.
 * Returns a new batch with a selection vector of matching rows.
 */
export class VectorizedFilter {
  /**
   * @param {Function} predicateFn — (batch: ColumnBatch) => Uint32Array (selected indices)
   */
  constructor(predicateFn) {
    this._predicate = predicateFn;
  }

  /** Apply filter to batch, return filtered batch. */
  execute(batch) {
    const selected = this._predicate(batch);
    return batch.filter(selected);
  }
}

/**
 * VectorizedProject — extract specific columns from a batch.
 */
export class VectorizedProject {
  constructor(columnNames) {
    this._columns = columnNames;
  }

  execute(batch) {
    const projected = new Map();
    for (const name of this._columns) {
      projected.set(name, batch.getColumn(name));
    }
    return new ColumnBatch(projected, batch.length, batch.selection);
  }
}

/**
 * VectorizedAggregate — compute aggregates over a batch.
 * Supports: COUNT, SUM, AVG, MIN, MAX.
 */
export class VectorizedAggregate {
  constructor(aggregates) {
    this._aggregates = aggregates; // [{fn: 'SUM', column: 'amount', alias: 'total'}]
  }

  execute(batch) {
    const results = {};
    
    for (const agg of this._aggregates) {
      const col = batch.getColumn(agg.column);
      const sel = batch.selection;
      
      switch (agg.fn.toUpperCase()) {
        case 'COUNT': {
          results[agg.alias] = sel ? sel.length : batch.length;
          break;
        }
        case 'SUM': {
          let sum = 0;
          if (sel) {
            for (const i of sel) if (col[i] != null) sum += col[i];
          } else {
            for (let i = 0; i < batch.length; i++) if (col[i] != null) sum += col[i];
          }
          results[agg.alias] = sum;
          break;
        }
        case 'AVG': {
          let sum = 0, count = 0;
          if (sel) {
            for (const i of sel) if (col[i] != null) { sum += col[i]; count++; }
          } else {
            for (let i = 0; i < batch.length; i++) if (col[i] != null) { sum += col[i]; count++; }
          }
          results[agg.alias] = count > 0 ? sum / count : null;
          break;
        }
        case 'MIN': {
          let min = Infinity;
          if (sel) {
            for (const i of sel) if (col[i] != null && col[i] < min) min = col[i];
          } else {
            for (let i = 0; i < batch.length; i++) if (col[i] != null && col[i] < min) min = col[i];
          }
          results[agg.alias] = min === Infinity ? null : min;
          break;
        }
        case 'MAX': {
          let max = -Infinity;
          if (sel) {
            for (const i of sel) if (col[i] != null && col[i] > max) max = col[i];
          } else {
            for (let i = 0; i < batch.length; i++) if (col[i] != null && col[i] > max) max = col[i];
          }
          results[agg.alias] = max === -Infinity ? null : max;
          break;
        }
      }
    }
    
    return results;
  }
}

/**
 * Build a vectorized filter predicate from a simple WHERE clause.
 * Returns a function (batch) => Uint32Array.
 */
export function buildFilterPredicate(column, op, value) {
  return (batch) => {
    const col = batch.getColumn(column);
    const selected = [];
    const n = batch.selection ? batch.selection.length : batch.length;
    
    const iterate = batch.selection || { length: batch.length, [Symbol.iterator]: function*() {
      for (let i = 0; i < batch.length; i++) yield i;
    }};
    
    for (const i of (batch.selection || Array.from({length: batch.length}, (_, i) => i))) {
      const v = col[i];
      let pass = false;
      switch (op) {
        case '>': pass = v > value; break;
        case '<': pass = v < value; break;
        case '>=': pass = v >= value; break;
        case '<=': pass = v <= value; break;
        case '=': case '==': pass = v === value; break;
        case '!=': case '<>': pass = v !== value; break;
      }
      if (pass) selected.push(i);
    }
    
    return new Uint32Array(selected);
  };
}

/**
 * Combine two filter predicates with AND.
 */
export function andPredicate(pred1, pred2) {
  return (batch) => {
    const sel1 = pred1(batch);
    const filtered = batch.filter(sel1);
    const sel2 = pred2(filtered);
    return sel2;
  };
}
