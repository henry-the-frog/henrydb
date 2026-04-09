// union-find.js — Disjoint Set (Union-Find) with path compression + union by rank
//
// Provides near-O(1) operations for:
//   - find(x): Which set does x belong to?
//   - union(x, y): Merge the sets containing x and y
//   - connected(x, y): Are x and y in the same set?
//
// Used in databases for:
//   - Join equivalence class detection
//   - Connected component analysis in graph queries
//   - Partition merging in distributed queries
//
// Amortized time per operation: O(α(n)) ≈ O(1) where α is inverse Ackermann

export class UnionFind {
  constructor(n = 0) {
    this._parent = new Array(n);
    this._rank = new Array(n);
    this._size = new Array(n);
    this._count = n; // Number of disjoint sets
    
    for (let i = 0; i < n; i++) {
      this._parent[i] = i;
      this._rank[i] = 0;
      this._size[i] = 1;
    }
  }

  /**
   * Number of elements.
   */
  get elements() { return this._parent.length; }

  /**
   * Number of disjoint sets.
   */
  get sets() { return this._count; }

  /**
   * Add a new element (returns its id).
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
   * Find the representative of x's set. O(α(n)).
   * Uses path compression.
   */
  find(x) {
    if (this._parent[x] !== x) {
      this._parent[x] = this.find(this._parent[x]); // Path compression
    }
    return this._parent[x];
  }

  /**
   * Merge the sets containing x and y. O(α(n)).
   * Uses union by rank.
   * Returns false if already in same set.
   */
  union(x, y) {
    let rootX = this.find(x);
    let rootY = this.find(y);
    
    if (rootX === rootY) return false; // Already connected
    
    // Union by rank: attach smaller tree to larger
    if (this._rank[rootX] < this._rank[rootY]) {
      [rootX, rootY] = [rootY, rootX];
    }
    
    this._parent[rootY] = rootX;
    this._size[rootX] += this._size[rootY];
    
    if (this._rank[rootX] === this._rank[rootY]) {
      this._rank[rootX]++;
    }
    
    this._count--;
    return true;
  }

  /**
   * Check if x and y are in the same set. O(α(n)).
   */
  connected(x, y) {
    return this.find(x) === this.find(y);
  }

  /**
   * Get the size of the set containing x.
   */
  setSize(x) {
    return this._size[this.find(x)];
  }

  /**
   * Get all sets as arrays of elements.
   */
  getAllSets() {
    const sets = new Map();
    for (let i = 0; i < this._parent.length; i++) {
      const root = this.find(i);
      if (!sets.has(root)) sets.set(root, []);
      sets.get(root).push(i);
    }
    return [...sets.values()];
  }
}
