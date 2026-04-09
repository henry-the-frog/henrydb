// segment-tree.js — Segment tree for range queries
//
// A segment tree enables O(log n) range queries (sum, min, max) and
// O(log n) point updates on an array. Used in databases for:
//   - Window function computation over large ranges
//   - Range aggregation optimization
//   - Histogram maintenance

export class SegmentTree {
  /**
   * @param {number[]} arr - Input array
   * @param {Function} combine - Combine function (default: sum)
   * @param {*} identity - Identity element (default: 0 for sum)
   */
  constructor(arr, combine, identity) {
    this.n = arr.length;
    this._combine = combine || ((a, b) => a + b);
    this._identity = identity ?? 0;
    this._tree = new Array(4 * this.n).fill(this._identity);
    
    if (arr.length > 0) this._build(arr, 1, 0, this.n - 1);
  }

  /**
   * Create a sum segment tree.
   */
  static sum(arr) {
    return new SegmentTree(arr, (a, b) => a + b, 0);
  }

  /**
   * Create a min segment tree.
   */
  static min(arr) {
    return new SegmentTree(arr, (a, b) => Math.min(a, b), Infinity);
  }

  /**
   * Create a max segment tree.
   */
  static max(arr) {
    return new SegmentTree(arr, (a, b) => Math.max(a, b), -Infinity);
  }

  /**
   * Query range [l, r] inclusive. O(log n).
   */
  query(l, r) {
    return this._query(1, 0, this.n - 1, l, r);
  }

  /**
   * Update position idx to new value. O(log n).
   */
  update(idx, value) {
    this._update(1, 0, this.n - 1, idx, value);
  }

  // --- Internal ---

  _build(arr, node, start, end) {
    if (start === end) {
      this._tree[node] = arr[start];
      return;
    }
    const mid = (start + end) >>> 1;
    this._build(arr, 2 * node, start, mid);
    this._build(arr, 2 * node + 1, mid + 1, end);
    this._tree[node] = this._combine(this._tree[2 * node], this._tree[2 * node + 1]);
  }

  _query(node, start, end, l, r) {
    if (r < start || end < l) return this._identity;
    if (l <= start && end <= r) return this._tree[node];
    const mid = (start + end) >>> 1;
    return this._combine(
      this._query(2 * node, start, mid, l, r),
      this._query(2 * node + 1, mid + 1, end, l, r)
    );
  }

  _update(node, start, end, idx, value) {
    if (start === end) {
      this._tree[node] = value;
      return;
    }
    const mid = (start + end) >>> 1;
    if (idx <= mid) this._update(2 * node, start, mid, idx, value);
    else this._update(2 * node + 1, mid + 1, end, idx, value);
    this._tree[node] = this._combine(this._tree[2 * node], this._tree[2 * node + 1]);
  }
}
