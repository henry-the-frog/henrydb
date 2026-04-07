// fenwick.js — Fenwick Tree (BIT) + Cuckoo Hash Table for HenryDB

/**
 * Fenwick Tree (Binary Indexed Tree).
 * O(log n) point updates, O(log n) prefix sum queries.
 * More space-efficient than segment tree for cumulative operations.
 */
export class FenwickTree {
  constructor(size) {
    this._n = size;
    this._tree = new Array(size + 1).fill(0);
  }

  /**
   * Add delta to position i (0-indexed).
   */
  update(i, delta) {
    i++; // Convert to 1-indexed
    while (i <= this._n) {
      this._tree[i] += delta;
      i += i & (-i); // Add lowest set bit
    }
  }

  /**
   * Get prefix sum [0, i] (0-indexed).
   */
  query(i) {
    i++; // Convert to 1-indexed
    let sum = 0;
    while (i > 0) {
      sum += this._tree[i];
      i -= i & (-i); // Remove lowest set bit
    }
    return sum;
  }

  /**
   * Get range sum [left, right] (0-indexed).
   */
  rangeQuery(left, right) {
    return this.query(right) - (left > 0 ? this.query(left - 1) : 0);
  }

  /**
   * Find the k-th smallest element (if tree represents frequencies).
   * Uses binary lifting for O(log n).
   */
  findKth(k) {
    let pos = 0;
    let bitMask = 1;
    while (bitMask <= this._n) bitMask <<= 1;
    bitMask >>= 1;
    
    while (bitMask > 0) {
      const next = pos + bitMask;
      if (next <= this._n && this._tree[next] < k) {
        k -= this._tree[next];
        pos = next;
      }
      bitMask >>= 1;
    }
    return pos; // 0-indexed
  }
}

/**
 * Cuckoo Hash Table.
 * O(1) worst-case lookup, amortized O(1) insert.
 * Uses two hash functions and two arrays.
 */
export class CuckooHashTable {
  constructor(capacity = 16) {
    this._capacity = capacity;
    this._table1 = new Array(capacity).fill(null);
    this._table2 = new Array(capacity).fill(null);
    this._size = 0;
    this._maxKicks = 500;
  }

  /**
   * Insert key-value pair. O(1) amortized.
   */
  insert(key, value) {
    // Check if already exists
    if (this.get(key) !== undefined) {
      this._updateExisting(key, value);
      return true;
    }

    let entry = { key, value };
    
    for (let i = 0; i < this._maxKicks; i++) {
      // Try table 1
      const h1 = this._hash1(entry.key) % this._capacity;
      if (this._table1[h1] === null) {
        this._table1[h1] = entry;
        this._size++;
        return true;
      }
      
      // Evict from table 1
      const evicted1 = this._table1[h1];
      this._table1[h1] = entry;
      entry = evicted1;
      
      // Try table 2
      const h2 = this._hash2(entry.key) % this._capacity;
      if (this._table2[h2] === null) {
        this._table2[h2] = entry;
        this._size++;
        return true;
      }
      
      // Evict from table 2
      const evicted2 = this._table2[h2];
      this._table2[h2] = entry;
      entry = evicted2;
    }
    
    // Too many kicks — resize and retry
    this._resize();
    return this.insert(entry.key, entry.value);
  }

  /**
   * Get value by key. O(1) worst-case.
   */
  get(key) {
    const h1 = this._hash1(key) % this._capacity;
    if (this._table1[h1] && this._table1[h1].key === key) {
      return this._table1[h1].value;
    }
    
    const h2 = this._hash2(key) % this._capacity;
    if (this._table2[h2] && this._table2[h2].key === key) {
      return this._table2[h2].value;
    }
    
    return undefined;
  }

  /**
   * Delete a key. O(1) worst-case.
   */
  delete(key) {
    const h1 = this._hash1(key) % this._capacity;
    if (this._table1[h1] && this._table1[h1].key === key) {
      this._table1[h1] = null;
      this._size--;
      return true;
    }
    
    const h2 = this._hash2(key) % this._capacity;
    if (this._table2[h2] && this._table2[h2].key === key) {
      this._table2[h2] = null;
      this._size--;
      return true;
    }
    
    return false;
  }

  get size() { return this._size; }

  _updateExisting(key, value) {
    const h1 = this._hash1(key) % this._capacity;
    if (this._table1[h1] && this._table1[h1].key === key) {
      this._table1[h1].value = value;
      return;
    }
    const h2 = this._hash2(key) % this._capacity;
    if (this._table2[h2] && this._table2[h2].key === key) {
      this._table2[h2].value = value;
    }
  }

  _resize() {
    const oldT1 = this._table1;
    const oldT2 = this._table2;
    this._capacity *= 2;
    this._table1 = new Array(this._capacity).fill(null);
    this._table2 = new Array(this._capacity).fill(null);
    this._size = 0;
    
    for (const entry of oldT1) {
      if (entry) this.insert(entry.key, entry.value);
    }
    for (const entry of oldT2) {
      if (entry) this.insert(entry.key, entry.value);
    }
  }

  _hash1(key) {
    const str = String(key);
    let hash = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }

  _hash2(key) {
    const str = String(key);
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) + hash) + str.charCodeAt(i);
    }
    return hash >>> 0;
  }
}
