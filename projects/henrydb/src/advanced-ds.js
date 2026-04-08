// advanced-ds.js — Advanced data structures: Fenwick tree, segment tree, union-find, suffix array, double hashing

/** Fenwick Tree (Binary Indexed Tree) — O(log n) point update + prefix sum */
export class FenwickTree {
  constructor(n) { this._tree = new Float64Array(n + 1); this.n = n; }
  update(i, delta) { for (i++; i <= this.n; i += i & -i) this._tree[i] += delta; }
  prefixSum(i) { let s = 0; for (i++; i > 0; i -= i & -i) s += this._tree[i]; return s; }
  rangeSum(l, r) { return this.prefixSum(r) - (l > 0 ? this.prefixSum(l - 1) : 0); }
  
  static fromArray(arr) {
    const ft = new FenwickTree(arr.length);
    for (let i = 0; i < arr.length; i++) ft.update(i, arr[i]);
    return ft;
  }
}

/** Segment Tree — O(log n) range queries with lazy propagation */
export class SegmentTree {
  constructor(arr, op = Math.min, identity = Infinity) {
    this.n = arr.length;
    this.op = op;
    this.identity = identity;
    this._tree = new Array(4 * this.n).fill(identity);
    if (this.n > 0) this._build(arr, 1, 0, this.n - 1);
  }
  _build(arr, node, start, end) {
    if (start === end) { this._tree[node] = arr[start]; return; }
    const mid = (start + end) >> 1;
    this._build(arr, 2 * node, start, mid);
    this._build(arr, 2 * node + 1, mid + 1, end);
    this._tree[node] = this.op(this._tree[2 * node], this._tree[2 * node + 1]);
  }
  update(idx, val) { this._update(1, 0, this.n - 1, idx, val); }
  _update(node, start, end, idx, val) {
    if (start === end) { this._tree[node] = val; return; }
    const mid = (start + end) >> 1;
    if (idx <= mid) this._update(2 * node, start, mid, idx, val);
    else this._update(2 * node + 1, mid + 1, end, idx, val);
    this._tree[node] = this.op(this._tree[2 * node], this._tree[2 * node + 1]);
  }
  query(l, r) { return this._query(1, 0, this.n - 1, l, r); }
  _query(node, start, end, l, r) {
    if (r < start || end < l) return this.identity;
    if (l <= start && end <= r) return this._tree[node];
    const mid = (start + end) >> 1;
    return this.op(this._query(2 * node, start, mid, l, r), this._query(2 * node + 1, mid + 1, end, l, r));
  }
}

/** Union-Find (Disjoint Set) — near O(1) amortized with path compression + union by rank */
export class UnionFind {
  constructor(n) { this.parent = Array.from({ length: n }, (_, i) => i); this.rank = new Array(n).fill(0); this._count = n; }
  find(x) { if (this.parent[x] !== x) this.parent[x] = this.find(this.parent[x]); return this.parent[x]; }
  union(x, y) {
    const px = this.find(x), py = this.find(y);
    if (px === py) return false;
    if (this.rank[px] < this.rank[py]) this.parent[px] = py;
    else if (this.rank[px] > this.rank[py]) this.parent[py] = px;
    else { this.parent[py] = px; this.rank[px]++; }
    this._count--;
    return true;
  }
  connected(x, y) { return this.find(x) === this.find(y); }
  get count() { return this._count; }
}

/** Suffix Array — sorted suffixes for O(m log n) substring search */
export class SuffixArray {
  constructor(text) {
    this.text = text;
    this.sa = this._build(text);
  }
  _build(text) {
    const n = text.length;
    const suffixes = Array.from({ length: n }, (_, i) => i);
    suffixes.sort((a, b) => {
      for (let k = 0; k < n; k++) {
        if (a + k >= n) return -1;
        if (b + k >= n) return 1;
        if (text[a + k] < text[b + k]) return -1;
        if (text[a + k] > text[b + k]) return 1;
      }
      return 0;
    });
    return suffixes;
  }
  /** Find all occurrences of pattern in text */
  search(pattern) {
    const positions = [];
    let lo = 0, hi = this.sa.length - 1;
    // Binary search for first match
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const suffix = this.text.substring(this.sa[mid], this.sa[mid] + pattern.length);
      if (suffix < pattern) lo = mid + 1;
      else hi = mid - 1;
    }
    // Collect all matches
    for (let i = lo; i < this.sa.length; i++) {
      if (this.text.substring(this.sa[i], this.sa[i] + pattern.length) === pattern) positions.push(this.sa[i]);
      else break;
    }
    return positions;
  }
}

/** Double Hashing — open addressing with two hash functions */
export class DoubleHashTable {
  constructor(capacity = 1024) {
    this._capacity = capacity;
    this._keys = new Array(capacity).fill(undefined);
    this._values = new Array(capacity).fill(undefined);
    this._occupied = new Uint8Array(capacity);
    this._size = 0;
  }
  set(key, value) {
    if (this._size >= this._capacity * 0.7) this._resize();
    const h1 = this._hash1(key), h2 = this._hash2(key);
    for (let i = 0; i < this._capacity; i++) {
      const idx = (h1 + i * h2) % this._capacity;
      if (!this._occupied[idx] || this._keys[idx] === key) {
        if (!this._occupied[idx]) this._size++;
        this._keys[idx] = key; this._values[idx] = value; this._occupied[idx] = 1;
        return;
      }
    }
  }
  get(key) {
    const h1 = this._hash1(key), h2 = this._hash2(key);
    for (let i = 0; i < this._capacity; i++) {
      const idx = (h1 + i * h2) % this._capacity;
      if (!this._occupied[idx]) return undefined;
      if (this._keys[idx] === key) return this._values[idx];
    }
    return undefined;
  }
  has(key) { return this.get(key) !== undefined; }
  get size() { return this._size; }
  _hash1(key) { let h = 0; const s = String(key); for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0; return (h >>> 0) % this._capacity; }
  _hash2(key) { let h = 0x12345; const s = String(key); for (let i = 0; i < s.length; i++) h = ((h * 31) + s.charCodeAt(i)) | 0; const v = ((h >>> 0) % (this._capacity - 2)); return v < 1 ? 1 : v; }
  _resize() {
    const old = { keys: this._keys, values: this._values, occupied: this._occupied, capacity: this._capacity };
    this._capacity *= 2; this._keys = new Array(this._capacity).fill(undefined); this._values = new Array(this._capacity).fill(undefined); this._occupied = new Uint8Array(this._capacity); this._size = 0;
    for (let i = 0; i < old.capacity; i++) { if (old.occupied[i]) this.set(old.keys[i], old.values[i]); }
  }
}
