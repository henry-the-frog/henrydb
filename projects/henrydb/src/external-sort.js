// external-sort.js — External sort for ORDER BY on large datasets
// When data exceeds the memory budget, split into sorted "runs",
// then k-way merge the runs to produce sorted output.
// In a real database, runs would be written to disk. Here we simulate
// with separate arrays to demonstrate the algorithm.

/**
 * ExternalSort — sorted-merge external sort.
 */
export class ExternalSort {
  constructor(options = {}) {
    this.runSize = options.runSize || 10000; // Max rows per in-memory sort run
    this.stats = { totalRows: 0, runs: 0, mergePassRows: 0, sortTimeMs: 0, mergeTimeMs: 0 };
  }

  /**
   * Sort an array of values using external sort.
   * @param {any[]} data — array to sort
   * @param {Function} comparator — comparison function (a, b) => number
   */
  sort(data, comparator) {
    this.stats.totalRows = data.length;

    if (data.length <= this.runSize) {
      // Fits in memory: just sort directly
      const t0 = Date.now();
      const result = [...data].sort(comparator);
      this.stats.sortTimeMs = Date.now() - t0;
      this.stats.runs = 1;
      return result;
    }

    // Phase 1: Create sorted runs
    const t0 = Date.now();
    const runs = [];
    for (let i = 0; i < data.length; i += this.runSize) {
      const run = data.slice(i, Math.min(i + this.runSize, data.length));
      run.sort(comparator);
      runs.push(run);
    }
    this.stats.sortTimeMs = Date.now() - t0;
    this.stats.runs = runs.length;

    // Phase 2: K-way merge
    const t1 = Date.now();
    const result = this._kWayMerge(runs, comparator);
    this.stats.mergeTimeMs = Date.now() - t1;

    return result;
  }

  /**
   * Sort rows by a column, using external sort.
   */
  sortByColumn(rows, column, direction = 'ASC') {
    const dir = direction === 'DESC' ? -1 : 1;
    return this.sort(rows, (a, b) => {
      const av = a[column], bv = b[column];
      if (av < bv) return -dir;
      if (av > bv) return dir;
      return 0;
    });
  }

  /**
   * Sort rows by multiple columns (e.g., ORDER BY region ASC, score DESC).
   */
  sortByColumns(rows, columns) {
    return this.sort(rows, (a, b) => {
      for (const { name, direction } of columns) {
        const dir = direction === 'DESC' ? -1 : 1;
        const av = a[name], bv = b[name];
        if (av < bv) return -dir;
        if (av > bv) return dir;
      }
      return 0;
    });
  }

  /**
   * K-way merge of sorted runs using a tournament tree (min-heap).
   */
  _kWayMerge(runs, comparator) {
    const k = runs.length;
    if (k === 0) return [];
    if (k === 1) return runs[0];
    if (k === 2) return this._twoWayMerge(runs[0], runs[1], comparator);

    // Use a min-heap for efficient k-way merge
    const result = [];
    const cursors = runs.map((run, i) => ({ runIdx: i, pos: 0 }));
    
    // Simple priority queue (heap would be better for large k)
    const active = cursors.filter(c => c.pos < runs[c.runIdx].length);

    while (active.length > 0) {
      // Find minimum element across all active runs
      let minIdx = 0;
      for (let i = 1; i < active.length; i++) {
        const c = active[i];
        const m = active[minIdx];
        if (comparator(runs[c.runIdx][c.pos], runs[m.runIdx][m.pos]) < 0) {
          minIdx = i;
        }
      }

      const cursor = active[minIdx];
      result.push(runs[cursor.runIdx][cursor.pos]);
      this.stats.mergePassRows++;
      cursor.pos++;

      // Remove exhausted runs
      if (cursor.pos >= runs[cursor.runIdx].length) {
        active.splice(minIdx, 1);
      }
    }

    return result;
  }

  /**
   * Optimized 2-way merge for the common binary merge case.
   */
  _twoWayMerge(a, b, comparator) {
    const result = new Array(a.length + b.length);
    let ai = 0, bi = 0, ri = 0;

    while (ai < a.length && bi < b.length) {
      if (comparator(a[ai], b[bi]) <= 0) {
        result[ri++] = a[ai++];
      } else {
        result[ri++] = b[bi++];
      }
      this.stats.mergePassRows++;
    }

    while (ai < a.length) { result[ri++] = a[ai++]; this.stats.mergePassRows++; }
    while (bi < b.length) { result[ri++] = b[bi++]; this.stats.mergePassRows++; }

    return result;
  }

  /**
   * Top-K sort: only return the first K elements (optimization for LIMIT).
   * Uses a max-heap of size K — O(n log K) instead of O(n log n).
   */
  topK(data, k, comparator) {
    if (k >= data.length) return this.sort(data, comparator);
    
    // Build max-heap of size K
    const heap = data.slice(0, k);
    heap.sort(comparator);

    for (let i = k; i < data.length; i++) {
      // If current element is smaller than the max in heap, swap
      if (comparator(data[i], heap[k - 1]) < 0) {
        heap[k - 1] = data[i];
        // Re-sort to maintain heap property (simple approach)
        // In production, use proper heap sift-down
        heap.sort(comparator);
      }
    }

    return heap;
  }

  getStats() {
    return {
      ...this.stats,
      avgRunSize: this.stats.runs > 0 ? Math.round(this.stats.totalRows / this.stats.runs) : 0,
    };
  }
}
