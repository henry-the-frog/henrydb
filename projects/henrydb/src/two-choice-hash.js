// two-choice-hash.js — Power of Two Choices hashing
// Insert into the less loaded of 2 random buckets → O(log log n) max load.
// Used in load balancing (Nginx, HAProxy).

export class TwoChoiceHash {
  constructor(buckets = 16) {
    this._buckets = Array.from({ length: buckets }, () => []);
    this._size = 0;
  }

  insert(key, value) {
    const h1 = this._hash1(key) % this._buckets.length;
    const h2 = this._hash2(key) % this._buckets.length;
    
    // Insert into less loaded bucket
    if (this._buckets[h1].length <= this._buckets[h2].length) {
      this._buckets[h1].push({ key, value });
    } else {
      this._buckets[h2].push({ key, value });
    }
    this._size++;
  }

  get(key) {
    const h1 = this._hash1(key) % this._buckets.length;
    const h2 = this._hash2(key) % this._buckets.length;
    
    for (const e of this._buckets[h1]) if (e.key === key) return e.value;
    for (const e of this._buckets[h2]) if (e.key === key) return e.value;
    return undefined;
  }

  maxLoad() { return Math.max(...this._buckets.map(b => b.length)); }
  avgLoad() { return this._size / this._buckets.length; }

  _hash1(key) { let h = 0; for (const c of String(key)) h = (h * 31 + c.charCodeAt(0)) >>> 0; return h; }
  _hash2(key) { let h = 0x9e3779b9; for (const c of String(key)) h = (h * 37 + c.charCodeAt(0)) >>> 0; return h; }
}
