// vectorized.js — Vectorized Execution Engine for HenryDB
// Inspired by MonetDB/X100 and CockroachDB's vectorized engine
//
// Instead of row-at-a-time (Volcano model), process batches of columnar data.
// Each operator produces/consumes "column batches" — arrays of typed values.

const BATCH_SIZE = 1024;

/**
 * ColumnBatch — a batch of columnar data.
 * Each column is a typed array (or regular array for strings).
 * length: number of valid rows in this batch (may be < BATCH_SIZE for last batch)
 */
export class ColumnBatch {
  constructor(columns, length) {
    this.columns = columns; // Array of arrays, one per column
    this.length = length;   // Number of valid rows
  }

  static empty(numCols) {
    const columns = Array.from({ length: numCols }, () => []);
    return new ColumnBatch(columns, 0);
  }

  /** Create a batch from rows (row-major → column-major conversion) */
  static fromRows(rows, numCols) {
    const columns = Array.from({ length: numCols }, () => new Array(rows.length));
    for (let r = 0; r < rows.length; r++) {
      for (let c = 0; c < numCols; c++) {
        columns[c][r] = rows[r][c];
      }
    }
    return new ColumnBatch(columns, rows.length);
  }

  /** Convert back to rows (for compatibility with existing code) */
  toRows() {
    const rows = [];
    const numCols = this.columns.length;
    for (let r = 0; r < this.length; r++) {
      const row = new Array(numCols);
      for (let c = 0; c < numCols; c++) {
        row[c] = this.columns[c][r];
      }
      rows.push(row);
    }
    return rows;
  }

  /** Get a single row (for debugging/compatibility) */
  getRow(idx) {
    return this.columns.map(col => col[idx]);
  }
}

// ============================================================
// OPERATORS — each has a next() that returns a ColumnBatch or null
// ============================================================

/**
 * VecScanOperator — reads from a data source and produces batches.
 * Wraps an iterator of rows.
 */
export class VecScanOperator {
  constructor(rows, numCols) {
    this._rows = rows;
    this._numCols = numCols;
    this._idx = 0;
  }

  next() {
    if (this._idx >= this._rows.length) return null;
    const end = Math.min(this._idx + BATCH_SIZE, this._rows.length);
    const batch = this._rows.slice(this._idx, end);
    this._idx = end;
    return ColumnBatch.fromRows(batch, this._numCols);
  }

  reset() { this._idx = 0; }
}

/**
 * VecFilterOperator — filters rows based on a predicate on a column.
 * Predicate operates on an entire column vector at once.
 */
export class VecFilterOperator {
  constructor(input, colIdx, op, value) {
    this.input = input;
    this.colIdx = colIdx;
    this.op = op;
    this.value = value;
    // Pre-compile the filter function for the specific operator
    this._filterFn = VecFilterOperator._compileFilter(op, value);
  }

  static _compileFilter(op, value) {
    switch (op) {
      case '=':  return (v) => v === value;
      case '!=': return (v) => v !== value;
      case '<':  return (v) => v < value;
      case '<=': return (v) => v <= value;
      case '>':  return (v) => v > value;
      case '>=': return (v) => v >= value;
      default: throw new Error(`Unknown filter op: ${op}`);
    }
  }

  next() {
    while (true) {
      const batch = this.input.next();
      if (!batch) return null;

      const col = batch.columns[this.colIdx];
      const fn = this._filterFn;
      const numCols = batch.columns.length;

      // Build selection vector (indices of matching rows)
      const selected = [];
      for (let i = 0; i < batch.length; i++) {
        if (fn(col[i])) selected.push(i);
      }

      if (selected.length === 0) continue;

      // Compact the batch using selection vector
      const newCols = Array.from({ length: numCols }, () => new Array(selected.length));
      for (let c = 0; c < numCols; c++) {
        const srcCol = batch.columns[c];
        const dstCol = newCols[c];
        for (let i = 0; i < selected.length; i++) {
          dstCol[i] = srcCol[selected[i]];
        }
      }

      return new ColumnBatch(newCols, selected.length);
    }
  }
}

/**
 * VecProjectOperator — computes new columns from existing ones.
 * Operates on entire column vectors at once.
 */
export class VecProjectOperator {
  constructor(input, projections) {
    this.input = input;
    // projections: array of {type: 'col', idx} or {type: 'expr', fn: (batch) => newCol}
    this.projections = projections;
  }

  next() {
    const batch = this.input.next();
    if (!batch) return null;

    const newCols = this.projections.map(proj => {
      if (proj.type === 'col') {
        return batch.columns[proj.idx];
      } else if (proj.type === 'expr') {
        return proj.fn(batch);
      }
      throw new Error(`Unknown projection type: ${proj.type}`);
    });

    return new ColumnBatch(newCols, batch.length);
  }
}

/**
 * VecArithmeticExpr — vectorized arithmetic on a column.
 * Returns a function that takes a batch and returns a new column array.
 */
export function vecMulScalar(colIdx, scalar) {
  return (batch) => {
    const src = batch.columns[colIdx];
    const dst = new Array(batch.length);
    for (let i = 0; i < batch.length; i++) {
      dst[i] = src[i] * scalar;
    }
    return dst;
  };
}

export function vecAddScalar(colIdx, scalar) {
  return (batch) => {
    const src = batch.columns[colIdx];
    const dst = new Array(batch.length);
    for (let i = 0; i < batch.length; i++) {
      dst[i] = src[i] + scalar;
    }
    return dst;
  };
}

export function vecAddCols(colIdxA, colIdxB) {
  return (batch) => {
    const a = batch.columns[colIdxA];
    const b = batch.columns[colIdxB];
    const dst = new Array(batch.length);
    for (let i = 0; i < batch.length; i++) {
      dst[i] = a[i] + b[i];
    }
    return dst;
  };
}

export function vecMulCols(colIdxA, colIdxB) {
  return (batch) => {
    const a = batch.columns[colIdxA];
    const b = batch.columns[colIdxB];
    const dst = new Array(batch.length);
    for (let i = 0; i < batch.length; i++) {
      dst[i] = a[i] * b[i];
    }
    return dst;
  };
}

/**
 * VecHashAggOperator — vectorized hash aggregation.
 * Groups by one column, computes aggregates on other columns.
 */
export class VecHashAggOperator {
  constructor(input, groupByCol, aggregates) {
    this.input = input;
    this.groupByCol = groupByCol;
    // aggregates: [{colIdx, fn: 'sum'|'count'|'min'|'max'|'avg'}]
    this.aggregates = aggregates;
    this._consumed = false;
    this._result = null;
  }

  next() {
    if (this._result) {
      const r = this._result;
      this._result = null;
      return r;
    }
    if (this._consumed) return null;
    this._consumed = true;

    // Consume all input batches
    const groups = new Map(); // groupKey → {count, agg state per aggregate}

    let batch;
    while ((batch = this.input.next()) !== null) {
      const groupCol = batch.columns[this.groupByCol];
      for (let i = 0; i < batch.length; i++) {
        const key = groupCol[i];
        if (!groups.has(key)) {
          groups.set(key, {
            count: 0,
            aggs: this.aggregates.map(a => {
              switch (a.fn) {
                case 'sum': return 0;
                case 'count': return 0;
                case 'min': return Infinity;
                case 'max': return -Infinity;
                case 'avg': return { sum: 0, count: 0 };
                default: return 0;
              }
            })
          });
        }
        const g = groups.get(key);
        g.count++;
        for (let a = 0; a < this.aggregates.length; a++) {
          const val = batch.columns[this.aggregates[a].colIdx][i];
          switch (this.aggregates[a].fn) {
            case 'sum': g.aggs[a] += val; break;
            case 'count': g.aggs[a]++; break;
            case 'min': if (val < g.aggs[a]) g.aggs[a] = val; break;
            case 'max': if (val > g.aggs[a]) g.aggs[a] = val; break;
            case 'avg': g.aggs[a].sum += val; g.aggs[a].count++; break;
          }
        }
      }
    }

    // Build result batch
    const numCols = 1 + this.aggregates.length;
    const rows = [];
    for (const [key, g] of groups) {
      const row = [key];
      for (let a = 0; a < this.aggregates.length; a++) {
        if (this.aggregates[a].fn === 'avg') {
          row.push(g.aggs[a].count > 0 ? g.aggs[a].sum / g.aggs[a].count : 0);
        } else {
          row.push(g.aggs[a]);
        }
      }
      rows.push(row);
    }

    if (rows.length === 0) return null;
    return ColumnBatch.fromRows(rows, numCols);
  }
}

/**
 * VecSortOperator — sorts all input by a column.
 * Materializes all batches, sorts, then emits in batch-sized chunks.
 */
export class VecSortOperator {
  constructor(input, sortColIdx, descending = false) {
    this.input = input;
    this.sortColIdx = sortColIdx;
    this.descending = descending;
    this._sorted = null;
    this._idx = 0;
  }

  next() {
    if (!this._sorted) {
      // Materialize all input
      const allRows = [];
      let numCols = 0;
      let batch;
      while ((batch = this.input.next()) !== null) {
        numCols = batch.columns.length;
        for (let i = 0; i < batch.length; i++) {
          allRows.push(batch.getRow(i));
        }
      }
      if (allRows.length === 0) return null;
      
      const col = this.sortColIdx;
      const dir = this.descending ? -1 : 1;
      allRows.sort((a, b) => {
        if (a[col] < b[col]) return -1 * dir;
        if (a[col] > b[col]) return 1 * dir;
        return 0;
      });
      
      this._sorted = allRows;
      this._numCols = numCols;
    }

    if (this._idx >= this._sorted.length) return null;
    const end = Math.min(this._idx + BATCH_SIZE, this._sorted.length);
    const chunk = this._sorted.slice(this._idx, end);
    this._idx = end;
    return ColumnBatch.fromRows(chunk, this._numCols);
  }
}

/**
 * VecLimitOperator — limits output to N rows.
 */
export class VecLimitOperator {
  constructor(input, limit) {
    this.input = input;
    this.limit = limit;
    this._emitted = 0;
  }

  next() {
    if (this._emitted >= this.limit) return null;
    const batch = this.input.next();
    if (!batch) return null;

    const remaining = this.limit - this._emitted;
    if (batch.length <= remaining) {
      this._emitted += batch.length;
      return batch;
    }

    // Truncate the batch
    const newCols = batch.columns.map(col => col.slice(0, remaining));
    this._emitted += remaining;
    return new ColumnBatch(newCols, remaining);
  }
}

/**
 * VecHashJoinOperator — vectorized hash join.
 * Builds hash table from right (build) side, probes with left (probe) side.
 */
export class VecHashJoinOperator {
  constructor(probeInput, buildInput, probeColIdx, buildColIdx) {
    this.probeInput = probeInput;
    this.buildInput = buildInput;
    this.probeColIdx = probeColIdx;
    this.buildColIdx = buildColIdx;
    this._hashTable = null;
    this._probeNumCols = 0;
    this._buildNumCols = 0;
  }

  _buildHashTable() {
    this._hashTable = new Map();
    let batch;
    while ((batch = this.buildInput.next()) !== null) {
      this._buildNumCols = batch.columns.length;
      const keyCol = batch.columns[this.buildColIdx];
      for (let i = 0; i < batch.length; i++) {
        const key = keyCol[i];
        if (!this._hashTable.has(key)) {
          this._hashTable.set(key, []);
        }
        this._hashTable.get(key).push(batch.getRow(i));
      }
    }
  }

  next() {
    if (!this._hashTable) this._buildHashTable();

    while (true) {
      const probeBatch = this.probeInput.next();
      if (!probeBatch) return null;

      this._probeNumCols = probeBatch.columns.length;
      const probeKeyCol = probeBatch.columns[this.probeColIdx];
      const outputRows = [];

      for (let i = 0; i < probeBatch.length; i++) {
        const key = probeKeyCol[i];
        const matches = this._hashTable.get(key);
        if (matches) {
          const probeRow = probeBatch.getRow(i);
          for (const buildRow of matches) {
            outputRows.push([...probeRow, ...buildRow]);
          }
        }
      }

      if (outputRows.length === 0) continue;
      return ColumnBatch.fromRows(outputRows, this._probeNumCols + this._buildNumCols);
    }
  }
}

// ============================================================
// PIPELINE — chain operators together for easy construction
// ============================================================

/**
 * Collect all output from an operator pipeline into rows.
 */
export function collectRows(operator) {
  const allRows = [];
  let batch;
  while ((batch = operator.next()) !== null) {
    for (let i = 0; i < batch.length; i++) {
      allRows.push(batch.getRow(i));
    }
  }
  return allRows;
}

/**
 * Count total rows from an operator without materializing.
 */
export function countRows(operator) {
  let total = 0;
  let batch;
  while ((batch = operator.next()) !== null) {
    total += batch.length;
  }
  return total;
}

export { BATCH_SIZE };
