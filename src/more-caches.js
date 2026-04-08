// more-caches.js — Advanced cache replacement policies: ARC, 2Q, CLOCK-Pro

/**
 * ARC (Adaptive Replacement Cache) — scan-resistant cache.
 * Maintains 4 lists: T1 (recent), T2 (frequent), B1 (ghost recent), B2 (ghost frequent).
 * Dynamically adjusts T1/T2 split based on workload.
 */
export class ARCCache {
  constructor(capacity) {
    this.c = capacity;
    this.p = 0; // Target size of T1
    this.T1 = new Map(); // Recent entries
    this.T2 = new Map(); // Frequent entries
    this.B1 = new Set(); // Ghost entries for T1
    this.B2 = new Set(); // Ghost entries for T2
    this.stats = { hits: 0, misses: 0 };
  }

  get(key) {
    if (this.T1.has(key)) {
      this.stats.hits++;
      const val = this.T1.get(key);
      this.T1.delete(key);
      this.T2.set(key, val); // Promote to frequent
      return val;
    }
    if (this.T2.has(key)) {
      this.stats.hits++;
      const val = this.T2.get(key);
      this.T2.delete(key);
      this.T2.set(key, val); // Move to MRU
      return val;
    }
    this.stats.misses++;
    return undefined;
  }

  put(key, value) {
    if (this.T1.has(key)) {
      this.T1.delete(key);
      this.T2.set(key, value);
      return;
    }
    if (this.T2.has(key)) {
      this.T2.delete(key);
      this.T2.set(key, value);
      return;
    }

    if (this.B1.has(key)) {
      // Adapt: increase T1 target
      this.p = Math.min(this.c, this.p + Math.max(1, Math.floor(this.B2.size / this.B1.size)));
      this._replace(key);
      this.B1.delete(key);
      this.T2.set(key, value);
      return;
    }

    if (this.B2.has(key)) {
      // Adapt: decrease T1 target
      this.p = Math.max(0, this.p - Math.max(1, Math.floor(this.B1.size / this.B2.size)));
      this._replace(key);
      this.B2.delete(key);
      this.T2.set(key, value);
      return;
    }

    // New entry
    if (this.T1.size + this.B1.size >= this.c) {
      if (this.T1.size < this.c) {
        // Remove LRU from B1
        const first = this.B1.values().next().value;
        this.B1.delete(first);
        this._replace(key);
      } else {
        // Remove LRU from T1
        const first = this.T1.keys().next().value;
        this.T1.delete(first);
      }
    } else if (this.T1.size + this.T2.size + this.B1.size + this.B2.size >= this.c) {
      if (this.T1.size + this.T2.size + this.B1.size + this.B2.size >= 2 * this.c) {
        const first = this.B2.values().next().value;
        if (first !== undefined) this.B2.delete(first);
      }
      this._replace(key);
    }
    this.T1.set(key, value);
  }

  _replace(key) {
    if (this.T1.size > 0 && (this.T1.size > this.p || (this.B2.has(key) && this.T1.size === this.p))) {
      const lru = this.T1.keys().next().value;
      this.T1.delete(lru);
      this.B1.add(lru);
    } else if (this.T2.size > 0) {
      const lru = this.T2.keys().next().value;
      this.T2.delete(lru);
      this.B2.add(lru);
    }
  }

  get size() { return this.T1.size + this.T2.size; }
  get hitRate() { const t = this.stats.hits + this.stats.misses; return t > 0 ? this.stats.hits / t : 0; }
}

/**
 * 2Q Cache — hot/cold page separation.
 * A1in (FIFO) → Am (LRU). A1out tracks recently evicted.
 */
export class TwoQCache {
  constructor(capacity) {
    this.capacity = capacity;
    this.Kin = Math.max(1, Math.floor(capacity * 0.25)); // 25% for A1in
    this.Km = capacity - this.Kin; // 75% for Am
    this.A1in = new Map(); // FIFO for new entries
    this.Am = new Map();   // LRU for frequently accessed
    this.A1out = new Set(); // Ghost entries (recently evicted from A1in)
    this.stats = { hits: 0, misses: 0 };
  }

  get(key) {
    if (this.Am.has(key)) {
      this.stats.hits++;
      const val = this.Am.get(key);
      this.Am.delete(key);
      this.Am.set(key, val); // LRU refresh
      return val;
    }
    if (this.A1in.has(key)) {
      this.stats.hits++;
      return this.A1in.get(key);
    }
    this.stats.misses++;
    return undefined;
  }

  put(key, value) {
    if (this.Am.has(key)) { this.Am.delete(key); this.Am.set(key, value); return; }
    if (this.A1in.has(key)) { this.A1in.set(key, value); return; }

    if (this.A1out.has(key)) {
      // Was recently evicted from A1in — promote to Am
      this.A1out.delete(key);
      if (this.Am.size >= this.Km) {
        const lru = this.Am.keys().next().value;
        this.Am.delete(lru);
      }
      this.Am.set(key, value);
      return;
    }

    // New entry → A1in
    if (this.A1in.size >= this.Kin) {
      const evicted = this.A1in.keys().next().value;
      this.A1in.delete(evicted);
      this.A1out.add(evicted);
      // Cap A1out
      if (this.A1out.size > this.capacity) {
        const oldest = this.A1out.values().next().value;
        this.A1out.delete(oldest);
      }
    }
    this.A1in.set(key, value);
  }

  get size() { return this.A1in.size + this.Am.size; }
  get hitRate() { const t = this.stats.hits + this.stats.misses; return t > 0 ? this.stats.hits / t : 0; }
}
