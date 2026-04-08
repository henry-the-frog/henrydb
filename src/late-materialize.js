// late-materialize.js — Late materialization for query execution
// Instead of constructing row objects at every step (scan → filter → join → project),
// pass around column references + index arrays. Only materialize at the final output.
// This eliminates GC pressure from millions of intermediate objects.
//
// A "virtual relation" is { columns: Map<name, array>, indices: Uint32Array }.
// The indices select which rows are "alive". Filter narrows indices. Join produces
// new index pairs. Only the final projection creates actual {key: value} objects.

/**
 * VirtualRelation — a lazy reference to columnar data.
 */
export class VirtualRelation {
  constructor(columns, indices = null) {
    this.columns = columns; // Map<string, any[]> or plain object {name: array}
    this._indices = indices; // null = all rows, Uint32Array = selected rows
    this._length = indices ? indices.length : this._inferLength();
  }

  _inferLength() {
    for (const [, arr] of Object.entries(this.columns)) {
      if (arr && arr.length !== undefined) return arr.length;
    }
    return 0;
  }

  get length() { return this._length; }

  /**
   * Filter: narrow the live rows by a predicate.
   * Returns a new VirtualRelation with fewer indices.
   */
  filter(columnName, predicate) {
    const col = this.columns[columnName];
    if (!col) throw new Error(`Column ${columnName} not found`);

    const newIndices = [];
    
    if (this._indices) {
      for (let i = 0; i < this._indices.length; i++) {
        const idx = this._indices[i];
        if (predicate(col[idx])) newIndices.push(idx);
      }
    } else {
      for (let i = 0; i < col.length; i++) {
        if (predicate(col[i])) newIndices.push(i);
      }
    }

    return new VirtualRelation(this.columns, new Uint32Array(newIndices));
  }

  /**
   * Filter with equality (most common case — optimized).
   */
  filterEquals(columnName, value) {
    return this.filter(columnName, v => v === value);
  }

  /**
   * Filter with comparison.
   */
  filterGT(columnName, value) {
    return this.filter(columnName, v => v > value);
  }

  filterLT(columnName, value) {
    return this.filter(columnName, v => v < value);
  }

  filterGE(columnName, value) {
    return this.filter(columnName, v => v >= value);
  }

  filterLE(columnName, value) {
    return this.filter(columnName, v => v <= value);
  }

  /**
   * Hash join with another VirtualRelation.
   * Returns a JoinedRelation (virtual, no materialization).
   */
  hashJoin(right, leftCol, rightCol, joinType = 'INNER') {
    const leftArr = this.columns[leftCol];
    const rightArr = right.columns[rightCol];
    if (!leftArr || !rightArr) throw new Error('Join column not found');

    // Build hash table on right side
    const ht = new Map();
    if (right._indices) {
      for (let i = 0; i < right._indices.length; i++) {
        const idx = right._indices[i];
        const key = rightArr[idx];
        if (!ht.has(key)) ht.set(key, []);
        ht.get(key).push(idx);
      }
    } else {
      for (let i = 0; i < rightArr.length; i++) {
        const key = rightArr[i];
        if (!ht.has(key)) ht.set(key, []);
        ht.get(key).push(i);
      }
    }

    // Probe
    const leftIndices = [];
    const rightIndices = [];
    const iterate = this._indices || Array.from({ length: leftArr.length }, (_, i) => i);

    for (const lIdx of iterate) {
      const key = leftArr[lIdx];
      const matches = ht.get(key);
      if (matches) {
        for (const rIdx of matches) {
          leftIndices.push(lIdx);
          rightIndices.push(rIdx);
        }
      } else if (joinType === 'LEFT' || joinType === 'LEFT OUTER') {
        leftIndices.push(lIdx);
        rightIndices.push(-1); // -1 = null row
      }
    }

    return new JoinedRelation(
      this.columns, right.columns,
      new Uint32Array(leftIndices), 
      new Int32Array(rightIndices) // Int32 for -1 support
    );
  }

  /**
   * Aggregate without materializing rows.
   */
  aggregate(groupByCol, aggCol, aggFn) {
    const groupArr = groupByCol ? this.columns[groupByCol] : null;
    const aggArr = this.columns[aggCol];
    const iterate = this._indices || Array.from({ length: this._length }, (_, i) => i);

    if (!groupArr) {
      // Whole-table aggregate
      let result;
      switch (aggFn) {
        case 'SUM': result = 0; for (const i of iterate) result += aggArr[i]; return [{ [aggFn.toLowerCase()]: result }];
        case 'COUNT': return [{ count: iterate.length }];
        case 'AVG': { let sum = 0; for (const i of iterate) sum += aggArr[i]; return [{ avg: sum / iterate.length }]; }
        case 'MIN': { let min = Infinity; for (const i of iterate) if (aggArr[i] < min) min = aggArr[i]; return [{ min }]; }
        case 'MAX': { let max = -Infinity; for (const i of iterate) if (aggArr[i] > max) max = aggArr[i]; return [{ max }]; }
      }
    }

    // Group-by aggregate
    const groups = new Map();
    for (const i of iterate) {
      const key = groupArr[i];
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(i);
    }

    const results = [];
    for (const [key, indices] of groups) {
      let value;
      switch (aggFn) {
        case 'SUM': value = 0; for (const i of indices) value += aggArr[i]; break;
        case 'COUNT': value = indices.length; break;
        case 'AVG': { let s = 0; for (const i of indices) s += aggArr[i]; value = s / indices.length; break; }
        case 'MIN': value = Infinity; for (const i of indices) if (aggArr[i] < value) value = aggArr[i]; break;
        case 'MAX': value = -Infinity; for (const i of indices) if (aggArr[i] > value) value = aggArr[i]; break;
      }
      results.push({ [groupByCol]: key, [aggFn.toLowerCase()]: value, count: indices.length });
    }
    return results;
  }

  /**
   * Materialize: project specific columns into row objects.
   * This is the ONLY step that creates actual objects.
   */
  materialize(columnNames, limit = Infinity) {
    const rows = [];
    const iterate = this._indices || Array.from({ length: this._length }, (_, i) => i);
    const colArrays = columnNames.map(name => [name, this.columns[name]]);

    for (let i = 0; i < iterate.length && rows.length < limit; i++) {
      const idx = iterate[i];
      const row = {};
      for (const [name, arr] of colArrays) {
        row[name] = arr ? arr[idx] : null;
      }
      rows.push(row);
    }

    return rows;
  }
}

/**
 * JoinedRelation — virtual relation resulting from a join.
 * Holds index pairs from left and right sides.
 */
export class JoinedRelation {
  constructor(leftColumns, rightColumns, leftIndices, rightIndices) {
    this.leftColumns = leftColumns;
    this.rightColumns = rightColumns;
    this.leftIndices = leftIndices;
    this.rightIndices = rightIndices;
    this._length = leftIndices.length;
  }

  get length() { return this._length; }

  /**
   * Materialize: project specific columns from both sides.
   * Format: { leftCol: value, rightCol: value, ... }
   */
  materialize(leftCols, rightCols, limit = Infinity) {
    const rows = [];
    const lArrays = leftCols.map(name => [name, this.leftColumns[name]]);
    const rArrays = rightCols.map(name => [name, this.rightColumns[name]]);

    for (let i = 0; i < this._length && rows.length < limit; i++) {
      const row = {};
      const lIdx = this.leftIndices[i];
      const rIdx = this.rightIndices[i];

      for (const [name, arr] of lArrays) {
        row[name] = arr ? arr[lIdx] : null;
      }
      for (const [name, arr] of rArrays) {
        const outName = name in row ? `right.${name}` : name;
        row[outName] = rIdx >= 0 && arr ? arr[rIdx] : null;
      }
      rows.push(row);
    }

    return rows;
  }

  /**
   * Materialize all columns.
   */
  materializeAll(limit = Infinity) {
    const leftCols = Object.keys(this.leftColumns);
    const rightCols = Object.keys(this.rightColumns);
    return this.materialize(leftCols, rightCols, limit);
  }
}
