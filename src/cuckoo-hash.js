// cuckoo-hash.js — Cuckoo hash table for O(1) worst-case lookups
// Uses two hash functions and two tables. Each key resides in exactly one of
// two positions. Lookup checks both positions — O(1) worst case.
// Insertion may trigger a chain of evictions (cuckoo moves).
//
// Advantages over chaining: no linked list traversal, cache-friendly,
// perfect for hash joins where probe-side performance matters most.

/**
 * CuckooHashTable — O(1) worst-case lookup hash table.
 */
export class CuckooHashTable {
  constructor(capacity = 1024) {
    this._capacity = capacity;
    this._table1 = new Array(capacity).fill(null); // { key, value }
    this._table2 = new Array(capacity).fill(null);
    this._size = 0;
    this._maxEvictions = 500; // Prevent infinite loops
    this.stats = { inserts: 0, evictions: 0, lookups: 0, resizes: 0 };
  }

  /**
   * Insert a key-value pair.
   */
  set(key, value) {
    // Check if key already exists
    const h1 = this._hash1(key);
    const h2 = this._hash2(key);

    if (this._table1[h1]?.key === key) {
      this._table1[h1].value = value;
      return;
    }
    if (this._table2[h2]?.key === key) {
      this._table2[h2].value = value;
      return;
    }

    // Insert with cuckoo eviction
    let entry = { key, value };
    for (let i = 0; i < this._maxEvictions; i++) {
      // Try table 1
      const pos1 = this._hash1(entry.key);
      if (!this._table1[pos1]) {
        this._table1[pos1] = entry;
        this._size++;
        this.stats.inserts++;
        return;
      }

      // Evict from table 1
      const evicted1 = this._table1[pos1];
      this._table1[pos1] = entry;
      entry = evicted1;
      this.stats.evictions++;

      // Try table 2
      const pos2 = this._hash2(entry.key);
      if (!this._table2[pos2]) {
        this._table2[pos2] = entry;
        this._size++;
        this.stats.inserts++;
        return;
      }

      // Evict from table 2
      const evicted2 = this._table2[pos2];
      this._table2[pos2] = entry;
      entry = evicted2;
      this.stats.evictions++;
    }

    // Too many evictions: resize and retry
    this._resize();
    this.set(entry.key, entry.value);
  }

  /**
   * Look up a key. Returns the value or undefined.
   * O(1) worst case — check exactly 2 positions.
   */
  get(key) {
    this.stats.lookups++;
    const h1 = this._hash1(key);
    if (this._table1[h1]?.key === key) return this._table1[h1].value;
    
    const h2 = this._hash2(key);
    if (this._table2[h2]?.key === key) return this._table2[h2].value;
    
    return undefined;
  }

  /**
   * Check if a key exists.
   */
  has(key) {
    return this.get(key) !== undefined;
  }

  /**
   * Delete a key.
   */
  delete(key) {
    const h1 = this._hash1(key);
    if (this._table1[h1]?.key === key) {
      this._table1[h1] = null;
      this._size--;
      return true;
    }

    const h2 = this._hash2(key);
    if (this._table2[h2]?.key === key) {
      this._table2[h2] = null;
      this._size--;
      return true;
    }

    return false;
  }

  get size() { return this._size; }

  _hash1(key) {
    let h = typeof key === 'number' ? key : 0;
    if (typeof key === 'string') {
      for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) | 0;
    }
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    return ((h >>> 16) ^ h) >>> 0 % this._capacity;
  }

  _hash2(key) {
    let h = typeof key === 'number' ? key : 0;
    if (typeof key === 'string') {
      for (let i = 0; i < key.length; i++) h = (h * 37 + key.charCodeAt(i)) | 0;
    }
    h = ((h >>> 16) ^ h) * 0x119de1f3;
    h = ((h >>> 16) ^ h) * 0x119de1f3;
    return ((h >>> 16) ^ h) >>> 0 % this._capacity;
  }

  _resize() {
    this.stats.resizes++;
    const oldTable1 = this._table1;
    const oldTable2 = this._table2;
    const oldCapacity = this._capacity;

    this._capacity *= 2;
    this._table1 = new Array(this._capacity).fill(null);
    this._table2 = new Array(this._capacity).fill(null);
    this._size = 0;

    for (let i = 0; i < oldCapacity; i++) {
      if (oldTable1[i]) this.set(oldTable1[i].key, oldTable1[i].value);
      if (oldTable2[i]) this.set(oldTable2[i].key, oldTable2[i].value);
    }
  }

  getStats() { return { ...this.stats, capacity: this._capacity, loadFactor: (this._size / this._capacity).toFixed(3) }; }
}
