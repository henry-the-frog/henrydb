// more-joins.js — Nested hash join, hash index, fractional cascading

/**
 * Nested Hash Join — multi-pass hash join for large tables.
 * Partitions build side into N passes, each fitting in memory.
 */
export class NestedHashJoin {
  constructor(memoryLimit = 1000) {
    this.memoryLimit = memoryLimit;
  }

  join(buildRows, probeRows, buildKey, probeKey) {
    const results = [];
    const numPasses = Math.ceil(buildRows.length / this.memoryLimit);
    
    for (let pass = 0; pass < numPasses; pass++) {
      const start = pass * this.memoryLimit;
      const end = Math.min(start + this.memoryLimit, buildRows.length);
      const partition = buildRows.slice(start, end);
      
      // Build hash table for this partition
      const ht = new Map();
      for (const row of partition) {
        const key = row[buildKey];
        if (!ht.has(key)) ht.set(key, []);
        ht.get(key).push(row);
      }
      
      // Probe
      for (const probeRow of probeRows) {
        const matches = ht.get(probeRow[probeKey]);
        if (matches) {
          for (const buildRow of matches) {
            results.push({ ...buildRow, ...probeRow });
          }
        }
      }
    }
    
    return results;
  }
}

/**
 * Hash Index — in-memory hash index with chaining.
 */
export class HashIndex {
  constructor(numBuckets = 256) {
    this.numBuckets = numBuckets;
    this.buckets = new Array(numBuckets).fill(null).map(() => []);
    this._size = 0;
  }

  _hash(key) {
    let h = 0;
    const s = String(key);
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
    return h % this.numBuckets;
  }

  insert(key, rowId) {
    const bucket = this.buckets[this._hash(key)];
    for (const entry of bucket) {
      if (entry.key === key) { entry.rowIds.push(rowId); this._size++; return; }
    }
    bucket.push({ key, rowIds: [rowId] });
    this._size++;
  }

  lookup(key) {
    const bucket = this.buckets[this._hash(key)];
    for (const entry of bucket) {
      if (entry.key === key) return entry.rowIds;
    }
    return [];
  }

  delete(key) {
    const h = this._hash(key);
    const idx = this.buckets[h].findIndex(e => e.key === key);
    if (idx >= 0) {
      this._size -= this.buckets[h][idx].rowIds.length;
      this.buckets[h].splice(idx, 1);
      return true;
    }
    return false;
  }

  /** Build from rows */
  static buildFromRows(rows, keyCol) {
    const idx = new HashIndex(Math.max(64, rows.length));
    for (let i = 0; i < rows.length; i++) idx.insert(rows[i][keyCol], i);
    return idx;
  }

  get size() { return this._size; }
  get loadFactor() { return this._size / this.numBuckets; }
}

/**
 * Fractional Cascading — speed up successive binary searches across multiple sorted lists.
 */
export class FractionalCascading {
  constructor(lists) {
    this.n = lists.length;
    this.augmented = this._build(lists);
  }

  _build(lists) {
    const aug = new Array(this.n);
    
    // Start from last list
    aug[this.n - 1] = lists[this.n - 1].map(v => ({ value: v, original: true }));
    
    // Build from back to front
    for (let i = this.n - 2; i >= 0; i--) {
      // Take every other element from aug[i+1]
      const promoted = [];
      for (let j = 0; j < aug[i + 1].length; j += 2) {
        promoted.push({ value: aug[i + 1][j].value, original: false });
      }
      
      // Merge with current list
      const current = lists[i].map(v => ({ value: v, original: true }));
      aug[i] = this._merge(current, promoted);
    }
    
    return aug;
  }

  _merge(a, b) {
    const result = [];
    let i = 0, j = 0;
    while (i < a.length && j < b.length) {
      result.push(a[i].value <= b[j].value ? a[i++] : b[j++]);
    }
    while (i < a.length) result.push(a[i++]);
    while (j < b.length) result.push(b[j++]);
    return result;
  }

  /** Search for a value across all lists */
  search(value) {
    const results = [];
    for (let i = 0; i < this.n; i++) {
      const list = this.augmented[i];
      // Binary search
      let lo = 0, hi = list.length;
      while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        list[mid].value < value ? lo = mid + 1 : hi = mid;
      }
      if (lo < list.length && list[lo].value === value && list[lo].original) {
        results.push({ listIndex: i, found: true });
      }
    }
    return results;
  }
}
