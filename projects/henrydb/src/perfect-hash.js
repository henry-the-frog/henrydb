// perfect-hash.js — Minimal perfect hash function
// Maps N keys to [0..N-1] with no collisions. O(N) build, O(1) lookup.
// Only works for static key sets (must know all keys at build time).

export class PerfectHash {
  constructor(keys) {
    this._keys = keys;
    this._n = keys.length;
    this._seeds = new Array(this._n).fill(0);
    this._table = new Array(this._n).fill(-1);
    this._build();
  }

  get(key) {
    const h = this._hash(key, 0) % this._n;
    const seed = this._seeds[h];
    return this._hash(key, seed) % this._n;
  }

  _build() {
    // Simple two-level hashing
    const buckets = Array.from({ length: this._n }, () => []);
    for (let i = 0; i < this._n; i++) {
      const h = this._hash(this._keys[i], 0) % this._n;
      buckets[h].push(i);
    }

    // Sort buckets by size descending
    const order = buckets.map((b, i) => [i, b]).sort((a, b) => b[1].length - a[1].length);

    for (const [bucketIdx, indices] of order) {
      if (indices.length === 0) continue;
      // Try different seeds until no collision
      for (let seed = 1; seed < 100; seed++) {
        const positions = indices.map(i => this._hash(this._keys[i], seed) % this._n);
        const valid = positions.every(p => this._table[p] === -1) &&
                      new Set(positions).size === positions.length;
        if (valid) {
          this._seeds[bucketIdx] = seed;
          for (let j = 0; j < indices.length; j++) {
            this._table[positions[j]] = indices[j];
          }
          break;
        }
      }
    }
  }

  _hash(key, seed) {
    let h = seed;
    const s = String(key);
    for (let i = 0; i < s.length; i++) {
      h = Math.imul(h ^ s.charCodeAt(i), 0x5bd1e995);
      h ^= h >>> 13;
    }
    return h >>> 0;
  }
}
