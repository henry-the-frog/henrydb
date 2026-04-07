// union-find.js — Disjoint-set (Union-Find) for HenryDB
// Used for: connected components in graph queries, query equivalence classes.

/**
 * Disjoint-Set with path compression and union by rank.
 * Near O(1) amortized operations (inverse Ackermann).
 */
export class UnionFind {
  constructor(size = 0) {
    this._parent = new Array(size).fill(0).map((_, i) => i);
    this._rank = new Array(size).fill(0);
    this._size = new Array(size).fill(1); // Component sizes
    this._count = size; // Number of components
  }

  /**
   * Add a new element. Returns its id.
   */
  makeSet() {
    const id = this._parent.length;
    this._parent.push(id);
    this._rank.push(0);
    this._size.push(1);
    this._count++;
    return id;
  }

  /**
   * Find the root (representative) of the set containing x.
   * Uses path compression for O(α(n)) amortized.
   */
  find(x) {
    if (this._parent[x] !== x) {
      this._parent[x] = this.find(this._parent[x]); // Path compression
    }
    return this._parent[x];
  }

  /**
   * Union two sets. Uses union by rank.
   * Returns true if they were in different sets.
   */
  union(x, y) {
    const rootX = this.find(x);
    const rootY = this.find(y);
    
    if (rootX === rootY) return false; // Already same set
    
    // Union by rank
    if (this._rank[rootX] < this._rank[rootY]) {
      this._parent[rootX] = rootY;
      this._size[rootY] += this._size[rootX];
    } else if (this._rank[rootX] > this._rank[rootY]) {
      this._parent[rootY] = rootX;
      this._size[rootX] += this._size[rootY];
    } else {
      this._parent[rootY] = rootX;
      this._size[rootX] += this._size[rootY];
      this._rank[rootX]++;
    }
    
    this._count--;
    return true;
  }

  /**
   * Check if x and y are in the same set.
   */
  connected(x, y) {
    return this.find(x) === this.find(y);
  }

  /**
   * Get the size of the component containing x.
   */
  componentSize(x) {
    return this._size[this.find(x)];
  }

  get componentCount() { return this._count; }

  /**
   * Get all components as arrays of elements.
   */
  getComponents() {
    const components = new Map();
    for (let i = 0; i < this._parent.length; i++) {
      const root = this.find(i);
      if (!components.has(root)) components.set(root, []);
      components.get(root).push(i);
    }
    return [...components.values()];
  }
}

/**
 * Sorted Set with rank queries.
 * Supports: insert, delete, rank(value), kth(k), range.
 * Uses a sorted array with binary search (O(n) insert/delete, O(log n) search/rank).
 */
export class SortedSet {
  constructor(comparator = (a, b) => a < b ? -1 : a > b ? 1 : 0) {
    this._data = [];
    this._cmp = comparator;
  }

  /**
   * Insert a value (maintains uniqueness).
   */
  insert(value) {
    const pos = this._bisect(value);
    if (pos < this._data.length && this._cmp(this._data[pos], value) === 0) return false;
    this._data.splice(pos, 0, value);
    return true;
  }

  /**
   * Delete a value.
   */
  delete(value) {
    const pos = this._bisect(value);
    if (pos < this._data.length && this._cmp(this._data[pos], value) === 0) {
      this._data.splice(pos, 1);
      return true;
    }
    return false;
  }

  /**
   * Check if value exists.
   */
  has(value) {
    const pos = this._bisect(value);
    return pos < this._data.length && this._cmp(this._data[pos], value) === 0;
  }

  /**
   * Get the rank (0-indexed position) of a value.
   */
  rank(value) {
    return this._bisect(value);
  }

  /**
   * Get the k-th element (0-indexed).
   */
  kth(k) {
    if (k < 0 || k >= this._data.length) return undefined;
    return this._data[k];
  }

  /**
   * Get the minimum element.
   */
  min() { return this._data[0]; }

  /**
   * Get the maximum element.
   */
  max() { return this._data[this._data.length - 1]; }

  /**
   * Range query: all elements in [low, high].
   */
  range(low, high) {
    const start = this._bisect(low);
    const results = [];
    for (let i = start; i < this._data.length; i++) {
      if (this._cmp(this._data[i], high) > 0) break;
      results.push(this._data[i]);
    }
    return results;
  }

  get size() { return this._data.length; }

  _bisect(value) {
    let lo = 0, hi = this._data.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._cmp(this._data[mid], value) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  [Symbol.iterator]() { return this._data[Symbol.iterator](); }
}
