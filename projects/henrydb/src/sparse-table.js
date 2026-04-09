// sparse-table.js — O(1) range minimum query after O(n log n) preprocess
// Immutable: build once, query many times. Great for static arrays.

export class SparseTable {
  constructor(arr, op, identity) {
    this._op = op || Math.min;
    this._identity = identity ?? Infinity;
    this._n = arr.length;
    this._log = new Array(arr.length + 1).fill(0);
    
    for (let i = 2; i <= arr.length; i++) this._log[i] = this._log[i >> 1] + 1;
    
    const K = this._log[arr.length] + 1;
    this._table = Array.from({ length: K }, () => new Array(arr.length));
    
    // Level 0: individual elements
    for (let i = 0; i < arr.length; i++) this._table[0][i] = arr[i];
    
    // Build: table[k][i] = op(table[k-1][i], table[k-1][i + 2^(k-1)])
    for (let k = 1; k < K; k++) {
      for (let i = 0; i + (1 << k) <= arr.length; i++) {
        this._table[k][i] = this._op(this._table[k-1][i], this._table[k-1][i + (1 << (k-1))]);
      }
    }
  }

  /** O(1) range query [l, r] inclusive. Works for idempotent ops (min, max, gcd). */
  query(l, r) {
    const k = this._log[r - l + 1];
    return this._op(this._table[k][l], this._table[k][r - (1 << k) + 1]);
  }

  static min(arr) { return new SparseTable(arr, Math.min, Infinity); }
  static max(arr) { return new SparseTable(arr, Math.max, -Infinity); }
}
