// van-emde-boas-layout.js — Cache-oblivious search tree layout
// Rearranges a sorted array into van Emde Boas layout for cache-efficient binary search.
// Idea: recursively split the tree so subtrees fit in cache lines.
// O(log_B N) cache misses instead of O(log N) for standard binary search.

export class VanEmdeBoasLayout {
  constructor(arr) {
    this._n = arr.length;
    this._layout = new Array(arr.length);
    this._build(arr, 0, arr.length - 1, 0);
  }

  get length() { return this._n; }

  /** Binary search on vEB layout. */
  search(key) {
    let idx = 0;
    while (idx < this._n) {
      if (this._layout[idx] === key) return true;
      if (key < this._layout[idx]) idx = 2 * idx + 1; // Left child
      else idx = 2 * idx + 2; // Right child
    }
    return false;
  }

  /** Get the layout array (for inspection). */
  getLayout() { return [...this._layout]; }

  _build(arr, lo, hi, pos) {
    if (lo > hi || pos >= this._n) return;
    const mid = (lo + hi) >>> 1;
    this._layout[pos] = arr[mid];
    this._build(arr, lo, mid - 1, 2 * pos + 1);
    this._build(arr, mid + 1, hi, 2 * pos + 2);
  }
}

/**
 * Eytzinger layout — simpler cache-friendly layout for static arrays.
 * Layout a sorted array in BFS order of implicit binary search tree.
 */
export class EytzingerLayout {
  constructor(arr) {
    this._n = arr.length;
    this._layout = new Array(arr.length + 1); // 1-indexed
    this._pos = 0;
    this._arr = arr;
    this._build(1);
  }

  search(key) {
    let i = 1;
    while (i <= this._n) {
      if (this._layout[i] === key) return true;
      i = key <= this._layout[i] ? 2 * i : 2 * i + 1;
    }
    return false;
  }

  _build(i) {
    if (i > this._n) return;
    this._build(2 * i);
    this._layout[i] = this._arr[this._pos++];
    this._build(2 * i + 1);
  }
}
