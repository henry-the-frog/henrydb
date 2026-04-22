// volcano.js — Volcano/Iterator execution engine for HenryDB
// Implements the classic open()/next()/close() iterator model
// Every operator is a node in a tree that pulls rows one at a time

/**
 * Base iterator class. All operators extend this.
 * Protocol:
 *   open()  — initialize the operator (allocate resources)
 *   next()  — return the next row, or null if exhausted
 *   close() — release resources
 */
export class Iterator {
  open() { throw new Error('Not implemented'); }
  next() { throw new Error('Not implemented'); }
  close() { /* default no-op */ }

  /** Describe this operator for EXPLAIN. Override in subclasses. */
  describe() { return { type: this.constructor.name, children: [], details: {} }; }

  /** Format as an indented tree string */
  explain(indent = 0) {
    const desc = this.describe();
    const prefix = '  '.repeat(indent);
    const details = Object.entries(desc.details || {})
      .filter(([, v]) => v != null)
      .map(([k, v]) => `${k}=${v}`)
      .join(', ');
    let line = `${prefix}→ ${desc.type}`;
    if (details) line += ` (${details})`;
    const lines = [line];
    for (const child of desc.children || []) {
      lines.push(child.explain(indent + 1));
    }
    return lines.join('\n');
  }

  /** Convenience: collect all rows into an array */
  toArray() {
    this.open();
    const rows = [];
    let row;
    while ((row = this.next()) !== null) {
      rows.push(row);
    }
    this.close();
    return rows;
  }

  /** Convenience: iterate with for...of */
  *[Symbol.iterator]() {
    this.open();
    let row;
    while ((row = this.next()) !== null) {
      yield row;
    }
    this.close();
  }
}

// ===== SeqScan — sequential scan of a heap =====

export class SeqScan extends Iterator {
  /**
   * @param {object} heap — HeapFile with scan() generator
   * @param {string[]} columns — column names for the schema
   * @param {string} [alias] — table alias for qualified column names
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

  next() {
    const { value, done } = this._gen.next();
    if (done) return null;
    // Convert [value1, value2, ...] array to {col: value} row object
    const row = {};
    for (let i = 0; i < this._columns.length; i++) {
      row[this._columns[i]] = value.values[i];
      if (this._alias) {
        row[`${this._alias}.${this._columns[i]}`] = value.values[i];
      }
    }
    // Attach physical location for potential updates
    row._pageId = value.pageId;
    row._slotIdx = value.slotIdx;
    return row;
  }

  close() {
    this._gen = null;
  }

  describe() {
    return {
      type: 'SeqScan',
      children: [],
      details: { table: this._alias || this._heap.name, columns: this._columns.join(', ') },
    };
  }
}

// ===== ValuesIter — iterate over literal row values =====

export class ValuesIter extends Iterator {
  constructor(rows) {
    super();
    this._rows = rows;
    this._idx = 0;
  }

  open() { this._idx = 0; }
  next() {
    if (this._idx >= this._rows.length) return null;
    return this._rows[this._idx++];
  }
  close() {}
  describe() {
    return { type: 'Values', children: [], details: { rows: this._rows.length } };
  }
}

// ===== Filter — predicate evaluation =====

export class Filter extends Iterator {
  /**
   * @param {Iterator} child — input iterator
   * @param {function} predicate — (row) => boolean
   */
  constructor(child, predicate) {
    super();
    this._child = child;
    this._predicate = predicate;
  }

  open() { this._child.open(); }

  next() {
    let row;
    while ((row = this._child.next()) !== null) {
      if (this._predicate(row)) return row;
    }
    return null;
  }

  close() { this._child.close(); }

  describe() {
    return { type: 'Filter', children: [this._child], details: {} };
  }
}

// ===== Project — column selection and expression evaluation =====

export class Project extends Iterator {
  /**
   * @param {Iterator} child — input iterator
   * @param {Array<{name: string, expr: function}>} projections
   *   Each projection has a name (output column) and an expr function (row) => value
   */
  constructor(child, projections) {
    super();
    this._child = child;
    this._projections = projections;
  }

  open() { this._child.open(); }

  next() {
    const row = this._child.next();
    if (row === null) return null;
    const out = {};
    for (const { name, expr } of this._projections) {
      out[name] = expr(row);
    }
    return out;
  }

  close() { this._child.close(); }

  describe() {
    return {
      type: 'Project',
      children: [this._child],
      details: { columns: this._projections.map(p => p.name).join(', ') },
    };
  }
}

// ===== Limit — early termination =====

export class Limit extends Iterator {
  constructor(child, limit, offset = 0) {
    super();
    this._child = child;
    this._limit = limit;
    this._offset = offset;
    this._count = 0;
    this._skipped = 0;
  }

  open() {
    this._count = 0;
    this._skipped = 0;
    this._child.open();
  }

  next() {
    // Skip offset rows
    while (this._skipped < this._offset) {
      const row = this._child.next();
      if (row === null) return null;
      this._skipped++;
    }
    if (this._count >= this._limit) return null;
    const row = this._child.next();
    if (row === null) return null;
    this._count++;
    return row;
  }

  close() { this._child.close(); }

  describe() {
    return {
      type: 'Limit',
      children: [this._child],
      details: { limit: this._limit, offset: this._offset || null },
    };
  }
}

// ===== Distinct — deduplicate rows =====

export class Distinct extends Iterator {
  /**
   * @param {Iterator} child
   * @param {string[]} [keys] — columns to check for distinctness (default: all)
   */
  constructor(child, keys) {
    super();
    this._child = child;
    this._keys = keys;
    this._seen = null;
  }

  open() {
    this._seen = new Set();
    this._child.open();
  }

  next() {
    let row;
    while ((row = this._child.next()) !== null) {
      const key = this._rowKey(row);
      if (!this._seen.has(key)) {
        this._seen.add(key);
        return row;
      }
    }
    return null;
  }

  close() {
    this._seen = null;
    this._child.close();
  }

  describe() {
    return {
      type: 'Distinct',
      children: [this._child],
      details: { keys: this._keys ? this._keys.join(', ') : 'all' },
    };
  }

  _rowKey(row) {
    if (this._keys) {
      return this._keys.map(k => JSON.stringify(row[k])).join('|');
    }
    // All columns (exclude internal _ prefixed)
    return Object.entries(row)
      .filter(([k]) => !k.startsWith('_'))
      .map(([k, v]) => `${k}:${JSON.stringify(v)}`)
      .join('|');
  }
}

// ===== NestedLoopJoin =====

export class NestedLoopJoin extends Iterator {
  /**
   * @param {Iterator} outer — left/outer input
   * @param {Iterator} inner — right/inner input (reopened per outer row)
   * @param {function} predicate — (outerRow, innerRow) => boolean (null = cross join)
   * @param {'inner'|'left'|'right'|'full'} [joinType='inner']
   */
  constructor(outer, inner, predicate, joinType = 'inner') {
    super();
    this._outer = outer;
    this._inner = inner;
    this._predicate = predicate;
    this._joinType = joinType;
    this._outerRow = null;
    this._matched = false;
    this._innerRows = null; // Materialized inner for re-scan
    this._matchedInner = null; // Track matched inner rows for RIGHT/FULL
    this._rightPhase = false; // Emitting unmatched inner rows
    this._rightIdx = 0;
  }

  open() {
    this._outer.open();
    // Materialize inner rows (since we re-scan per outer row)
    this._inner.open();
    this._innerRows = [];
    let row;
    while ((row = this._inner.next()) !== null) {
      this._innerRows.push(row);
    }
    this._inner.close();
    this._outerRow = null;
    this._innerIdx = 0;
    this._matched = false;
    this._rightPhase = false;
    this._rightIdx = 0;
    if (this._joinType === 'right' || this._joinType === 'full') {
      this._matchedInner = new Set();
    }
  }

  next() {
    // Right phase: emit unmatched inner rows
    if (this._rightPhase) {
      while (this._rightIdx < this._innerRows.length) {
        const idx = this._rightIdx++;
        if (!this._matchedInner.has(idx)) {
          const nullOuter = {};
          if (this._outerCols) {
            for (const col of this._outerCols) nullOuter[col] = null;
          }
          return { ...nullOuter, ...this._innerRows[idx] };
        }
      }
      return null;
    }

    while (true) {
      if (this._outerRow === null) {
        this._outerRow = this._outer.next();
        if (this._outerRow === null) {
          // Outer exhausted — enter right phase if RIGHT or FULL
          if ((this._joinType === 'right' || this._joinType === 'full') && this._matchedInner) {
            this._rightPhase = true;
            return this.next();
          }
          return null;
        }
        if (!this._outerCols) {
          this._outerCols = Object.keys(this._outerRow).filter(k => !k.startsWith('_'));
        }
        this._innerIdx = 0;
        this._matched = false;
      }

      while (this._innerIdx < this._innerRows.length) {
        const idx = this._innerIdx++;
        const innerRow = this._innerRows[idx];
        if (!this._predicate || this._predicate(this._outerRow, innerRow)) {
          this._matched = true;
          if (this._matchedInner) this._matchedInner.add(idx);
          return { ...this._outerRow, ...innerRow };
        }
      }

      // Inner exhausted for this outer row
      if ((this._joinType === 'left' || this._joinType === 'full') && !this._matched) {
        // Left/full join: emit outer row with nulls for inner columns
        const nullInner = {};
        if (this._innerRows.length > 0) {
          for (const key of Object.keys(this._innerRows[0])) {
            if (!key.startsWith('_')) nullInner[key] = null;
          }
        }
        const result = { ...this._outerRow, ...nullInner };
        this._outerRow = null;
        return result;
      }

      this._outerRow = null; // Move to next outer row
    }
  }

  close() {
    this._outer.close();
    this._innerRows = null;
  }

  describe() {
    return {
      type: 'NestedLoopJoin',
      children: [this._outer, this._inner],
      details: { joinType: this._joinType },
    };
  }
}

// ===== HashJoin =====

export class HashJoin extends Iterator {
  /**
   * @param {Iterator} build — build side (hashed)
   * @param {Iterator} probe — probe side
   * @param {string} buildKey — column name on build side
   * @param {string} probeKey — column name on probe side
   * @param {'inner'|'left'|'right'|'full'} [joinType='inner']
   */
  constructor(build, probe, buildKey, probeKey, joinType = 'inner') {
    super();
    this._build = build;
    this._probe = probe;
    this._buildKey = buildKey;
    this._probeKey = probeKey;
    this._joinType = joinType;
    this._hashTable = null;
    this._probeRow = null;
    this._matchIdx = 0;
    this._matches = null;
    this._buildCols = null;
    this._probeCols = null;
    this._matchedBuildRows = null; // Track matched build rows for RIGHT/FULL
    this._rightPhase = false; // Emitting unmatched build rows
    this._rightIter = null;
  }

  open() {
    // Build phase: hash the build side
    this._hashTable = new Map();
    this._build.open();
    this._allBuildRows = [];
    let row;
    while ((row = this._build.next()) !== null) {
      const key = row[this._buildKey];
      const keyStr = String(key);
      if (!this._hashTable.has(keyStr)) {
        this._hashTable.set(keyStr, []);
      }
      this._hashTable.get(keyStr).push(row);
      this._allBuildRows.push(row);
      if (!this._buildCols) this._buildCols = Object.keys(row).filter(k => !k.startsWith('_'));
    }
    this._build.close();

    // Probe phase
    this._probe.open();
    this._probeRow = null;
    this._matches = null;
    this._matchIdx = 0;
    this._rightPhase = false;
    this._rightIdx = 0;
    if (this._joinType === 'right' || this._joinType === 'full') {
      this._matchedBuildRows = new Set();
    }
  }

  next() {
    // Right phase: emit unmatched build rows
    if (this._rightPhase) {
      while (this._rightIdx < this._allBuildRows.length) {
        const idx = this._rightIdx++;
        if (!this._matchedBuildRows.has(this._allBuildRows[idx])) {
          const nullProbe = {};
          if (this._probeCols) {
            for (const col of this._probeCols) nullProbe[col] = null;
          }
          return { ...nullProbe, ...this._allBuildRows[idx] };
        }
      }
      return null;
    }

    while (true) {
      // Emit pending matches
      if (this._matches && this._matchIdx < this._matches.length) {
        const buildRow = this._matches[this._matchIdx++];
        if (this._matchedBuildRows) this._matchedBuildRows.add(buildRow);
        return { ...this._probeRow, ...buildRow };
      }

      // Get next probe row
      this._probeRow = this._probe.next();
      if (this._probeRow === null) {
        // Probe exhausted — enter right phase if RIGHT or FULL
        if ((this._joinType === 'right' || this._joinType === 'full') && this._matchedBuildRows) {
          this._rightPhase = true;
          return this.next();
        }
        return null;
      }
      if (!this._probeCols) {
        this._probeCols = Object.keys(this._probeRow).filter(k => !k.startsWith('_'));
      }

      const key = String(this._probeRow[this._probeKey]);
      this._matches = this._hashTable.get(key) || [];
      this._matchIdx = 0;

      if (this._matches.length === 0 && (this._joinType === 'left' || this._joinType === 'full')) {
        const nullBuild = {};
        if (this._buildCols) {
          for (const col of this._buildCols) nullBuild[col] = null;
        }
        return { ...this._probeRow, ...nullBuild };
      }
    }
  }

  close() {
    this._probe.close();
    this._hashTable = null;
    this._allBuildRows = null;
    this._matchedBuildRows = null;
  }

  describe() {
    return {
      type: 'HashJoin',
      children: [this._probe, this._build],
      details: { buildKey: this._buildKey, probeKey: this._probeKey, joinType: this._joinType },
    };
  }
}

// ===== MergeJoin =====

export class MergeJoin extends Iterator {
  /**
   * Both inputs must be sorted on join key.
   * @param {Iterator} left — sorted left input
   * @param {Iterator} right — sorted right input
   * @param {string} leftKey — left join column
   * @param {string} rightKey — right join column
   */
  constructor(left, right, leftKey, rightKey) {
    super();
    this._left = left;
    this._right = right;
    this._leftKey = leftKey;
    this._rightKey = rightKey;
    this._leftRow = null;
    this._rightRow = null;
    this._rightGroup = [];
    this._rightGroupIdx = 0;
    this._rightGroupKey = null;
  }

  open() {
    this._left.open();
    this._right.open();
    this._leftRow = this._left.next();
    this._rightRow = this._right.next();
    this._rightGroup = [];
    this._rightGroupIdx = 0;
    this._rightGroupKey = null;
  }

  next() {
    while (true) {
      // Emit from current group
      if (this._rightGroupIdx < this._rightGroup.length && this._leftRow) {
        const combined = { ...this._leftRow, ...this._rightGroup[this._rightGroupIdx++] };
        return combined;
      }

      if (!this._leftRow || !this._rightRow) return null;

      const lk = this._leftRow[this._leftKey];
      const rk = this._rightRow[this._rightKey];

      if (lk < rk) {
        this._leftRow = this._left.next();
        continue;
      }
      if (lk > rk) {
        this._rightRow = this._right.next();
        continue;
      }

      // Equal: collect right group with same key
      this._rightGroup = [];
      this._rightGroupKey = rk;
      while (this._rightRow && this._rightRow[this._rightKey] === this._rightGroupKey) {
        this._rightGroup.push(this._rightRow);
        this._rightRow = this._right.next();
      }
      this._rightGroupIdx = 0;

      // Emit first match
      if (this._rightGroup.length > 0) {
        const combined = { ...this._leftRow, ...this._rightGroup[this._rightGroupIdx++] };
        // If group exhausted for this left row, advance left
        if (this._rightGroupIdx >= this._rightGroup.length) {
          this._leftRow = this._left.next();
          // Check if next left row matches same group
          if (this._leftRow && this._leftRow[this._leftKey] === this._rightGroupKey) {
            this._rightGroupIdx = 0; // Re-scan group
          }
        }
        return combined;
      }
    }
  }

  close() {
    this._left.close();
    this._right.close();
  }

  describe() {
    return {
      type: 'MergeJoin',
      children: [this._left, this._right],
      details: { leftKey: this._leftKey, rightKey: this._rightKey },
    };
  }
}

// ===== Sort =====

export class Sort extends Iterator {
  /**
   * @param {Iterator} child
   * @param {Array<{column: string, desc?: boolean}>} orderBy
   */
  constructor(child, orderBy) {
    super();
    this._child = child;
    this._orderBy = orderBy;
    this._sorted = null;
    this._idx = 0;
  }

  open() {
    // Materialize and sort (blocking operator)
    this._child.open();
    this._sorted = [];
    let row;
    while ((row = this._child.next()) !== null) {
      this._sorted.push(row);
    }
    this._child.close();

    this._sorted.sort((a, b) => {
      for (const { column, desc } of this._orderBy) {
        const av = a[column], bv = b[column];
        let cmp = 0;
        if (av == null && bv == null) cmp = 0;
        else if (av == null) cmp = -1;
        else if (bv == null) cmp = 1;
        else if (typeof av === 'string') cmp = av.localeCompare(bv);
        else cmp = av - bv;
        if (cmp !== 0) return desc ? -cmp : cmp;
      }
      return 0;
    });
    this._idx = 0;
  }

  next() {
    if (this._idx >= this._sorted.length) return null;
    return this._sorted[this._idx++];
  }

  close() { this._sorted = null; }

  describe() {
    return {
      type: 'Sort',
      children: [this._child],
      details: { orderBy: this._orderBy.map(o => `${o.column}${o.desc ? ' DESC' : ''}`).join(', ') },
    };
  }
}

// ===== HashAggregate =====

export class HashAggregate extends Iterator {
  /**
   * @param {Iterator} child
   * @param {string[]} groupBy — group-by column names
   * @param {Array<{name: string, func: string, column: string}>} aggregates
   *   func: 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
   */
  constructor(child, groupBy, aggregates) {
    super();
    this._child = child;
    this._groupBy = groupBy;
    this._aggregates = aggregates;
    this._groups = null;
    this._groupIter = null;
  }

  open() {
    // Build hash table of groups (blocking)
    this._child.open();
    this._groups = new Map();
    let row;
    while ((row = this._child.next()) !== null) {
      const key = this._groupBy.map(c => JSON.stringify(row[c])).join('|');
      if (!this._groups.has(key)) {
        const group = { key, values: {}, aggs: {} };
        for (const col of this._groupBy) group.values[col] = row[col];
        for (const agg of this._aggregates) {
          group.aggs[agg.name] = { func: agg.func, values: [] };
        }
        this._groups.set(key, group);
      }
      const group = this._groups.get(key);
      for (const agg of this._aggregates) {
        const val = agg.column === '*' ? 1 : row[agg.column];
        group.aggs[agg.name].values.push(val);
      }
    }
    this._child.close();
    this._groupIter = this._groups.values();
  }

  next() {
    const { value, done } = this._groupIter.next();
    if (done) return null;
    
    const row = { ...value.values };
    for (const [name, { func, values }] of Object.entries(value.aggs)) {
      switch (func.toUpperCase()) {
        case 'COUNT':
          row[name] = values.filter(v => v != null).length;
          break;
        case 'SUM':
          row[name] = values.reduce((a, b) => (a || 0) + (b || 0), 0);
          break;
        case 'AVG': {
          const nonNull = values.filter(v => v != null);
          row[name] = nonNull.length ? nonNull.reduce((a, b) => a + b, 0) / nonNull.length : null;
          break;
        }
        case 'MIN':
          row[name] = values.filter(v => v != null).reduce((a, b) => a < b ? a : b, Infinity);
          if (row[name] === Infinity) row[name] = null;
          break;
        case 'MAX':
          row[name] = values.filter(v => v != null).reduce((a, b) => a > b ? a : b, -Infinity);
          if (row[name] === -Infinity) row[name] = null;
          break;
      }
    }
    return row;
  }

  close() {
    this._groups = null;
    this._groupIter = null;
  }

  describe() {
    return {
      type: 'HashAggregate',
      children: [this._child],
      details: {
        groupBy: this._groupBy.join(', ') || 'none',
        aggregates: this._aggregates.map(a => `${a.func}(${a.column}) AS ${a.name}`).join(', '),
      },
    };
  }
}

// ===== IndexScan =====

export class IndexScan extends Iterator {
  /**
   * @param {object} index — B+tree index with range() method
   * @param {object} heap — HeapFile for fetching full rows
   * @param {string[]} columns — column names
   * @param {*} [low] — range lower bound
   * @param {*} [high] — range upper bound
   * @param {string} [alias] — table alias
   */
  constructor(index, heap, columns, low, high, alias) {
    super();
    this._index = index;
    this._heap = heap;
    this._columns = columns;
    this._low = low;
    this._high = high;
    this._alias = alias;
    this._gen = null;
  }

  open() {
    // B+tree range() now handles undefined bounds (treated as open-ended)
    this._gen = this._index.range(this._low, this._high);
    this._results = [];
    // Collect index entries and fetch from heap
    for (const entry of this._gen) {
      const rid = entry.value || entry;
      if (rid && rid.pageId !== undefined) {
        const values = this._heap.get(rid.pageId, rid.slotIdx);
        if (values) {
          const row = {};
          for (let i = 0; i < this._columns.length; i++) {
            row[this._columns[i]] = values[i];
            if (this._alias) row[`${this._alias}.${this._columns[i]}`] = values[i];
          }
          row._pageId = rid.pageId;
          row._slotIdx = rid.slotIdx;
          this._results.push(row);
        }
      }
    }
    this._idx = 0;
  }

  next() {
    if (this._idx >= this._results.length) return null;
    return this._results[this._idx++];
  }

  close() {
    this._results = null;
    this._gen = null;
  }

  describe() {
    return {
      type: 'IndexScan',
      children: [],
      details: { table: this._alias, low: this._low, high: this._high },
    };
  }
}

// ===== Union =====

export class Union extends Iterator {
  constructor(left, right) {
    super();
    this._left = left;
    this._right = right;
    this._current = null;
  }

  open() {
    this._left.open();
    this._current = this._left;
  }

  next() {
    let row = this._current.next();
    if (row !== null) return row;
    if (this._current === this._left) {
      this._left.close();
      this._right.open();
      this._current = this._right;
      return this._current.next();
    }
    return null;
  }

  close() {
    if (this._current === this._left) this._left.close();
    else this._right.close();
  }

  describe() {
    return { type: 'Union', children: [this._left, this._right], details: {} };
  }
}

// ===== IndexNestedLoopJoin =====

export class IndexNestedLoopJoin extends Iterator {
  /**
   * For each outer row, probe the inner table's B+tree index on the join key.
   * Much faster than NLJ or HashJoin when inner table has an index.
   * 
   * @param {Iterator} outer — outer/driving iterator
   * @param {object} innerIndex — B+tree index on inner table's join column
   * @param {object} innerHeap — HeapFile for the inner table
   * @param {string[]} innerColumns — column names for inner table
   * @param {string} outerKey — column name on outer side to use as lookup key
   * @param {string} innerAlias — alias for inner table's qualified column names
   */
  constructor(outer, innerIndex, innerHeap, innerColumns, outerKey, innerAlias) {
    super();
    this._outer = outer;
    this._innerIndex = innerIndex;
    this._innerHeap = innerHeap;
    this._innerColumns = innerColumns;
    this._outerKey = outerKey;
    this._innerAlias = innerAlias;
    this._outerRow = null;
    this._pendingInner = [];
    this._pendingIdx = 0;
  }

  open() {
    this._outer.open();
    this._outerRow = null;
    this._pendingInner = [];
    this._pendingIdx = 0;
  }

  next() {
    while (true) {
      // Emit pending matches from current outer row
      if (this._pendingIdx < this._pendingInner.length) {
        return { ...this._outerRow, ...this._pendingInner[this._pendingIdx++] };
      }

      // Get next outer row
      this._outerRow = this._outer.next();
      if (this._outerRow === null) return null;

      // Probe index with outer key value
      const lookupKey = this._outerRow[this._outerKey];
      if (lookupKey == null) {
        this._pendingInner = [];
        this._pendingIdx = 0;
        continue;
      }

      // B+tree search returns a single RID for exact match
      const rid = this._innerIndex.search(lookupKey);
      this._pendingInner = [];
      this._pendingIdx = 0;

      if (rid && rid.pageId !== undefined) {
        // Single match — fetch the row
        const values = this._innerHeap.get(rid.pageId, rid.slotIdx);
        if (values) {
          this._pendingInner.push(this._buildInnerRow(values, rid));
        }
      }

      // For non-unique indexes, also check range for duplicate keys
      // The B+tree with suffix keys stores duplicates, so we need range scan
      if (!this._innerIndex.unique) {
        // Range scan for all entries matching this key
        try {
          for (const entry of this._innerIndex.range(lookupKey, lookupKey)) {
            const key = entry.key;
            const val = entry.value;
            if (val && val.pageId !== undefined) {
              // Only add if not already in the list (from search)
              const values = this._innerHeap.get(val.pageId, val.slotIdx);
              if (values) {
                const row = this._buildInnerRow(values, val);
                // Avoid duplicates (search result)
                if (rid && val.pageId === rid.pageId && val.slotIdx === rid.slotIdx) continue;
                this._pendingInner.push(row);
              }
            }
          }
        } catch (e) {
          // range() might not support equal bounds; fall back to search result
        }
      }
    }
  }

  close() {
    this._outer.close();
  }

  _buildInnerRow(values, rid) {
    const row = {};
    for (let i = 0; i < this._innerColumns.length; i++) {
      row[this._innerColumns[i]] = values[i];
      if (this._innerAlias) {
        row[`${this._innerAlias}.${this._innerColumns[i]}`] = values[i];
      }
    }
    row._pageId = rid.pageId;
    row._slotIdx = rid.slotIdx;
    return row;
  }

  describe() {
    return {
      type: 'IndexNestedLoopJoin',
      children: [this._outer],
      details: { outerKey: this._outerKey, innerAlias: this._innerAlias, indexLookup: true },
    };
  }
}

// ===== Window =====

export class Window extends Iterator {
  constructor(child, partitionBy, orderBy, windowFuncs) {
    super();
    this._child = child;
    this._partitionBy = partitionBy;
    this._orderBy = orderBy;
    this._windowFuncs = windowFuncs;
    this._results = null;
    this._idx = 0;
  }

  open() {
    this._child.open();
    const allRows = [];
    let row;
    while ((row = this._child.next()) !== null) allRows.push(row);
    this._child.close();

    const partitions = new Map();
    for (const r of allRows) {
      const key = this._partitionBy.map(c => JSON.stringify(r[c])).join('|');
      if (!partitions.has(key)) partitions.set(key, []);
      partitions.get(key).push(r);
    }

    this._results = [];
    for (const rows of partitions.values()) {
      let rank = 1, denseRank = 1, prevOrderKey = null;

      for (let i = 0; i < rows.length; i++) {
        const r = { ...rows[i] };
        const orderKey = this._orderBy.map(o => JSON.stringify(r[o.column])).join('|');

        for (const wf of this._windowFuncs) {
          switch (wf.func.toUpperCase()) {
            case 'ROW_NUMBER': r[wf.name] = i + 1; break;
            case 'RANK':
              if (i === 0 || orderKey !== prevOrderKey) rank = i + 1;
              r[wf.name] = rank; break;
            case 'DENSE_RANK':
              if (i === 0) denseRank = 1;
              else if (orderKey !== prevOrderKey) denseRank++;
              r[wf.name] = denseRank; break;
            case 'LAG': {
              const off = wf.offset || 1;
              r[wf.name] = i >= off ? rows[i - off][wf.arg] : (wf.defaultValue ?? null); break;
            }
            case 'LEAD': {
              const off = wf.offset || 1;
              r[wf.name] = i + off < rows.length ? rows[i + off][wf.arg] : (wf.defaultValue ?? null); break;
            }
            case 'SUM': { let s = 0; for (let j = 0; j <= i; j++) s += rows[j][wf.arg] || 0; r[wf.name] = s; break; }
            case 'COUNT': r[wf.name] = i + 1; break;
            case 'AVG': { let s = 0; for (let j = 0; j <= i; j++) s += rows[j][wf.arg] || 0; r[wf.name] = s / (i + 1); break; }
            case 'MIN': { let m = Infinity; for (let j = 0; j <= i; j++) { const v = rows[j][wf.arg]; if (v != null && v < m) m = v; } r[wf.name] = m === Infinity ? null : m; break; }
            case 'MAX': { let m = -Infinity; for (let j = 0; j <= i; j++) { const v = rows[j][wf.arg]; if (v != null && v > m) m = v; } r[wf.name] = m === -Infinity ? null : m; break; }
          }
        }
        prevOrderKey = orderKey;
        this._results.push(r);
      }
    }
    this._idx = 0;
  }

  next() {
    if (this._idx >= this._results.length) return null;
    return this._results[this._idx++];
  }

  close() { this._results = null; }

  describe() {
    return {
      type: 'Window',
      children: [this._child],
      details: {
        partitionBy: this._partitionBy.join(', ') || 'none',
        orderBy: this._orderBy.map(o => `${o.column}${o.desc ? ' DESC' : ''}`).join(', '),
        functions: this._windowFuncs.map(w => `${w.func}(${w.arg || ''}) AS ${w.name}`).join(', '),
      },
    };
  }
}

// ===== CTE (Common Table Expression) =====

export class CTE extends Iterator {
  /**
   * Non-recursive CTE: materialize a subquery, then use it as input.
   * @param {Iterator} definition — the CTE subquery
   * @param {Iterator} mainQuery — the main query that references the CTE
   * @param {string} cteName — name of the CTE (for reference)
   */
  constructor(definition, mainQuery, cteName) {
    super();
    this._definition = definition;
    this._mainQuery = mainQuery;
    this._cteName = cteName;
    this._materializedRows = null;
  }

  open() {
    // Materialize the CTE definition
    this._definition.open();
    this._materializedRows = [];
    let row;
    while ((row = this._definition.next()) !== null) {
      this._materializedRows.push(row);
    }
    this._definition.close();
    // Open the main query (which should use the materialized rows)
    this._mainQuery.open();
  }

  next() { return this._mainQuery.next(); }
  close() { this._mainQuery.close(); this._materializedRows = null; }

  getMaterialized() { return this._materializedRows; }

  describe() {
    return {
      type: 'CTE',
      children: [this._definition, this._mainQuery],
      details: { name: this._cteName },
    };
  }
}

// ===== RecursiveCTE =====

export class RecursiveCTE extends Iterator {
  /**
   * Recursive CTE: base case UNION ALL recursive step.
   * Iterates until recursive step produces no new rows.
   * 
   * @param {Iterator} baseCaseIter — the initial (non-recursive) query
   * @param {function} recursiveStepFactory — (currentRows: Row[]) => Iterator
   *   Given the current working table, returns an iterator for the next level
   * @param {number} [maxDepth=100] — safety limit to prevent infinite recursion
   */
  constructor(baseCaseIter, recursiveStepFactory, maxDepth = 100) {
    super();
    this._baseCaseIter = baseCaseIter;
    this._recursiveStepFactory = recursiveStepFactory;
    this._maxDepth = maxDepth;
    this._allRows = null;
    this._idx = 0;
  }

  open() {
    // Compute base case
    this._baseCaseIter.open();
    this._allRows = [];
    let row;
    while ((row = this._baseCaseIter.next()) !== null) {
      this._allRows.push(row);
    }
    this._baseCaseIter.close();

    // Iterate recursive step
    let workingTable = [...this._allRows];
    let depth = 0;

    while (workingTable.length > 0 && depth < this._maxDepth) {
      const stepIter = this._recursiveStepFactory(workingTable);
      stepIter.open();
      const newRows = [];
      while ((row = stepIter.next()) !== null) {
        newRows.push(row);
      }
      stepIter.close();

      if (newRows.length === 0) break;
      this._allRows.push(...newRows);
      workingTable = newRows;
      depth++;
    }

    this._idx = 0;
  }

  next() {
    if (this._idx >= this._allRows.length) return null;
    return this._allRows[this._idx++];
  }

  close() { this._allRows = null; }

  describe() {
    return {
      type: 'RecursiveCTE',
      children: [this._baseCaseIter],
      details: { maxDepth: this._maxDepth },
    };
  }
}

/**
 * Instrumented iterator wrapper — measures execution time and row count.
 * Used by EXPLAIN ANALYZE.
 */
export class InstrumentedIterator extends Iterator {
  constructor(inner) {
    super();
    this._inner = inner;
    this.rowCount = 0;
    this.openTimeMs = 0;
    this.nextTimeMs = 0;
    this.closeTimeMs = 0;
    this.totalTimeMs = 0;
    this._estimatedRows = inner._estimatedRows || null;
    // Recursively instrument children
    this._instrumentChildren();
  }
  
  _instrumentChildren() {
    const desc = this._inner.describe();
    if (!desc) return;
    // Replace child references with instrumented versions
    if (desc.children) {
      for (const child of desc.children) {
        // Children are already instrumented during plan traversal
      }
    }
  }
  
  open() {
    const t0 = performance.now();
    this._inner.open();
    this.openTimeMs = performance.now() - t0;
  }
  
  next() {
    const t0 = performance.now();
    const row = this._inner.next();
    const elapsed = performance.now() - t0;
    this.nextTimeMs += elapsed;
    if (row !== null) this.rowCount++;
    return row;
  }
  
  close() {
    const t0 = performance.now();
    this._inner.close();
    this.closeTimeMs = performance.now() - t0;
    this.totalTimeMs = this.openTimeMs + this.nextTimeMs + this.closeTimeMs;
  }
  
  describe() {
    const innerDesc = this._inner.describe();
    return {
      ...innerDesc,
      instrumented: true,
      rowCount: this.rowCount,
      openTimeMs: this.openTimeMs,
      nextTimeMs: this.nextTimeMs,
      totalTimeMs: this.totalTimeMs,
    };
  }
  
  explain(indent = 0) {
    const desc = this._inner.describe();
    const prefix = '  '.repeat(indent) + (indent > 0 ? '→ ' : '→ ');
    const type = desc.type || 'Unknown';
    const details = desc.details || {};
    const detailStr = Object.entries(details).map(([k, v]) => `${k}=${v}`).join(', ');
    
    const estStr = this._estimatedRows != null ? `est=${this._estimatedRows} ` : '';
    const timing = `(${estStr}actual=${this.rowCount} time=${this.totalTimeMs.toFixed(2)}ms)`;
    
    let line = `${prefix}${type}${detailStr ? ` (${detailStr})` : ''} ${timing}`;
    
    if (desc.children) {
      for (const child of desc.children) {
        if (child instanceof InstrumentedIterator) {
          line += '\n' + child.explain(indent + 1);
        }
      }
    }
    
    return line;
  }
}

/**
 * Recursively wrap a Volcano plan tree with InstrumentedIterator wrappers.
 */
export function instrumentPlan(iter) {
  if (!iter || !(iter instanceof Iterator)) return iter;
  
  // Instrument children first
  const desc = iter.describe();
  if (desc && desc.children) {
    for (let i = 0; i < desc.children.length; i++) {
      const child = desc.children[i];
      if (child instanceof Iterator) {
        const instrumented = instrumentPlan(child);
        // Replace child reference on the parent
        // This requires knowing the internal field names
        replaceChild(iter, child, instrumented);
      }
    }
  }
  
  return new InstrumentedIterator(iter);
}

function replaceChild(parent, oldChild, newChild) {
  // Try common field names for Volcano iterators
  const fields = ['_input', '_child', '_outer', '_inner', '_probe', '_build', 
                   '_baseCaseIter', '_source', '_left', '_right'];
  for (const field of fields) {
    if (parent[field] === oldChild) {
      parent[field] = newChild;
      return true;
    }
  }
  // Try array fields
  if (parent._inputs) {
    const idx = parent._inputs.indexOf(oldChild);
    if (idx !== -1) { parent._inputs[idx] = newChild; return true; }
  }
  return false;
}
