// fenwick-tree.js — Binary Indexed Tree (Fenwick Tree)
// O(log n) prefix sum queries and point updates using only n+1 elements.
// More space-efficient than segment tree for cumulative operations.

export class FenwickTree {
  constructor(n) {
    this.n = n;
    this._tree = new Float64Array(n + 1);
  }

  static fromArray(arr) {
    const ft = new FenwickTree(arr.length);
    for (let i = 0; i < arr.length; i++) ft.update(i, arr[i]);
    return ft;
  }

  /** Add delta to position i. O(log n). */
  update(i, delta) {
    i++; // 1-indexed
    while (i <= this.n) {
      this._tree[i] += delta;
      i += i & (-i);
    }
  }

  /** Prefix sum [0..i] inclusive. O(log n). */
  prefixSum(i) {
    i++; // 1-indexed
    let sum = 0;
    while (i > 0) {
      sum += this._tree[i];
      i -= i & (-i);
    }
    return sum;
  }

  /** Range sum [l..r] inclusive. O(log n). */
  rangeSum(l, r) {
    return this.prefixSum(r) - (l > 0 ? this.prefixSum(l - 1) : 0);
  }
}
