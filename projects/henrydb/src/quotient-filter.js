// quotient-filter.js — Space-efficient probabilistic set membership
// Like Bloom filter but: supports counting, merging, and resizing.
// Stores quotient (high bits) and remainder (low bits) of hash.
// Used in: SSD firmware, network dedup, database query optimization.

export class QuotientFilter {
  /**
   * @param {number} qBits - Quotient bits (filter has 2^qBits slots)
   */
  constructor(qBits = 10) {
    this._q = qBits;
    this._r = 32 - qBits;
    this._size = 1 << qBits;
    this._slots = new Array(this._size).fill(null); // {remainder, occupied, continuation, shifted}
    this._count = 0;
  }

  get size() { return this._count; }
  get capacity() { return this._size; }
  get loadFactor() { return this._count / this._size; }

  /**
   * Insert an element.
   */
  insert(key) {
    const hash = this._hash(String(key));
    const quotient = hash >>> this._r;
    const remainder = hash & ((1 << this._r) - 1);
    
    let slot = quotient % this._size;
    
    // Find empty slot (linear probing)
    let probes = 0;
    while (this._slots[slot] !== null && probes < this._size) {
      slot = (slot + 1) % this._size;
      probes++;
    }
    
    if (probes >= this._size) return false; // Full
    
    this._slots[slot] = { remainder, quotient };
    this._count++;
    return true;
  }

  /**
   * Check if element might exist (probabilistic).
   */
  contains(key) {
    const hash = this._hash(String(key));
    const quotient = hash >>> this._r;
    const remainder = hash & ((1 << this._r) - 1);
    
    let slot = quotient % this._size;
    let probes = 0;
    
    while (this._slots[slot] !== null && probes < this._size) {
      const s = this._slots[slot];
      if (s.quotient === quotient && s.remainder === remainder) return true;
      slot = (slot + 1) % this._size;
      probes++;
    }
    
    return false;
  }

  /**
   * Merge two quotient filters.
   */
  static merge(a, b) {
    const result = new QuotientFilter(a._q);
    for (const slot of a._slots) {
      if (slot) result._insertDirect(slot.quotient, slot.remainder);
    }
    for (const slot of b._slots) {
      if (slot) result._insertDirect(slot.quotient, slot.remainder);
    }
    return result;
  }

  _insertDirect(quotient, remainder) {
    let slot = quotient % this._size;
    let probes = 0;
    while (this._slots[slot] !== null && probes < this._size) {
      slot = (slot + 1) % this._size;
      probes++;
    }
    if (probes < this._size) {
      this._slots[slot] = { remainder, quotient };
      this._count++;
    }
  }

  _hash(str) {
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = (h * 0x01000193) >>> 0;
    }
    return h;
  }

  getStats() {
    return {
      count: this._count,
      capacity: this._size,
      loadFactor: this.loadFactor,
      quotientBits: this._q,
      remainderBits: this._r,
    };
  }
}
