// interval-tree.js — Interval tree + Min-Heap for HenryDB

/**
 * Min-Heap (Priority Queue).
 * Used for ORDER BY ... LIMIT K (top-K queries) without sorting all rows.
 */
export class MinHeap {
  constructor(comparator = (a, b) => a - b) {
    this._data = [];
    this._cmp = comparator;
  }

  push(item) {
    this._data.push(item);
    this._bubbleUp(this._data.length - 1);
  }

  pop() {
    if (this._data.length === 0) return undefined;
    const min = this._data[0];
    const last = this._data.pop();
    if (this._data.length > 0) {
      this._data[0] = last;
      this._sinkDown(0);
    }
    return min;
  }

  peek() { return this._data[0]; }
  get size() { return this._data.length; }

  _bubbleUp(i) {
    while (i > 0) {
      const parent = (i - 1) >>> 1;
      if (this._cmp(this._data[i], this._data[parent]) < 0) {
        [this._data[i], this._data[parent]] = [this._data[parent], this._data[i]];
        i = parent;
      } else break;
    }
  }

  _sinkDown(i) {
    const n = this._data.length;
    while (true) {
      let smallest = i;
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      if (left < n && this._cmp(this._data[left], this._data[smallest]) < 0) smallest = left;
      if (right < n && this._cmp(this._data[right], this._data[smallest]) < 0) smallest = right;
      if (smallest !== i) {
        [this._data[i], this._data[smallest]] = [this._data[smallest], this._data[i]];
        i = smallest;
      } else break;
    }
  }
}

/**
 * Interval Tree for overlapping range queries.
 * Stores intervals [low, high] and efficiently finds all intervals overlapping a point or range.
 */
export class IntervalTree {
  constructor() {
    this._intervals = []; // Simple sorted-list implementation
    this._size = 0;
  }

  /**
   * Insert an interval [low, high] with associated data.
   */
  insert(low, high, data = null) {
    this._intervals.push({ low, high, data });
    this._intervals.sort((a, b) => a.low - b.low);
    this._size++;
  }

  /**
   * Find all intervals containing a point.
   */
  queryPoint(point) {
    return this._intervals.filter(i => i.low <= point && point <= i.high);
  }

  /**
   * Find all intervals overlapping with [queryLow, queryHigh].
   */
  queryRange(queryLow, queryHigh) {
    return this._intervals.filter(i => i.low <= queryHigh && queryLow <= i.high);
  }

  /**
   * Find all intervals completely contained within [queryLow, queryHigh].
   */
  queryContained(queryLow, queryHigh) {
    return this._intervals.filter(i => i.low >= queryLow && i.high <= queryHigh);
  }

  /**
   * Remove intervals matching a predicate.
   */
  remove(predicate) {
    const before = this._intervals.length;
    this._intervals = this._intervals.filter(i => !predicate(i));
    this._size = this._intervals.length;
    return before - this._size;
  }

  get size() { return this._size; }
}
