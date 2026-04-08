// robin-hood-hash.js — Robin Hood hash table
// Open addressing with Robin Hood heuristic: during insertion, if the
// displacement of the current entry is less than that of the entry being
// inserted, swap them (the "rich" entry gives its position to the "poor" one).
// This equalizes probe sequence lengths, giving more predictable performance.

/**
 * RobinHoodHashTable — open addressing with displacement-based stealing.
 */
export class RobinHoodHashTable {
  constructor(capacity = 1024) {
    this._capacity = this._nextPow2(capacity);
    this._mask = this._capacity - 1;
    this._keys = new Array(this._capacity).fill(undefined);
    this._values = new Array(this._capacity).fill(undefined);
    this._displacements = new Int32Array(this._capacity); // -1 = empty
    this._displacements.fill(-1);
    this._size = 0;
    this._maxDisplacement = 0;
    this.stats = { inserts: 0, lookups: 0, probes: 0, steals: 0, resizes: 0 };
  }

  set(key, value) {
    if (this._size >= this._capacity * 0.7) this._resize();

    let pos = this._hash(key) & this._mask;
    let displacement = 0;
    let k = key, v = value;

    while (true) {
      if (this._displacements[pos] === -1) {
        // Empty slot
        this._keys[pos] = k;
        this._values[pos] = v;
        this._displacements[pos] = displacement;
        this._size++;
        this.stats.inserts++;
        if (displacement > this._maxDisplacement) this._maxDisplacement = displacement;
        return;
      }

      if (this._keys[pos] === k) {
        // Update existing
        this._values[pos] = v;
        return;
      }

      // Robin Hood: steal from rich entries
      if (this._displacements[pos] < displacement) {
        // Current occupant has shorter displacement — swap
        const tmpK = this._keys[pos];
        const tmpV = this._values[pos];
        const tmpD = this._displacements[pos];
        this._keys[pos] = k;
        this._values[pos] = v;
        this._displacements[pos] = displacement;
        k = tmpK;
        v = tmpV;
        displacement = tmpD;
        this.stats.steals++;
      }

      pos = (pos + 1) & this._mask;
      displacement++;
    }
  }

  get(key) {
    this.stats.lookups++;
    let pos = this._hash(key) & this._mask;
    let displacement = 0;

    while (true) {
      if (this._displacements[pos] === -1) return undefined;
      if (this._displacements[pos] < displacement) return undefined; // Robin Hood: can stop early
      
      this.stats.probes++;
      if (this._keys[pos] === key) return this._values[pos];

      pos = (pos + 1) & this._mask;
      displacement++;
    }
  }

  has(key) { return this.get(key) !== undefined; }

  delete(key) {
    let pos = this._hash(key) & this._mask;
    let displacement = 0;

    while (true) {
      if (this._displacements[pos] === -1) return false;
      if (this._displacements[pos] < displacement) return false;
      
      if (this._keys[pos] === key) {
        // Found: backward shift delete
        this._backwardShift(pos);
        this._size--;
        return true;
      }

      pos = (pos + 1) & this._mask;
      displacement++;
    }
  }

  _backwardShift(pos) {
    let next = (pos + 1) & this._mask;
    while (this._displacements[next] > 0) {
      this._keys[pos] = this._keys[next];
      this._values[pos] = this._values[next];
      this._displacements[pos] = this._displacements[next] - 1;
      pos = next;
      next = (next + 1) & this._mask;
    }
    this._keys[pos] = undefined;
    this._values[pos] = undefined;
    this._displacements[pos] = -1;
  }

  get size() { return this._size; }

  _hash(key) {
    let h = typeof key === 'number' ? key | 0 : 0;
    if (typeof key === 'string') {
      for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) | 0;
    }
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    return (h >>> 16) ^ h;
  }

  _nextPow2(n) {
    let p = 1;
    while (p < n) p <<= 1;
    return p;
  }

  _resize() {
    this.stats.resizes++;
    const oldKeys = this._keys;
    const oldValues = this._values;
    const oldDisp = this._displacements;
    const oldCapacity = this._capacity;

    this._capacity *= 2;
    this._mask = this._capacity - 1;
    this._keys = new Array(this._capacity).fill(undefined);
    this._values = new Array(this._capacity).fill(undefined);
    this._displacements = new Int32Array(this._capacity);
    this._displacements.fill(-1);
    this._size = 0;
    this._maxDisplacement = 0;

    for (let i = 0; i < oldCapacity; i++) {
      if (oldDisp[i] !== -1) this.set(oldKeys[i], oldValues[i]);
    }
  }

  getStats() {
    return {
      ...this.stats,
      capacity: this._capacity,
      size: this._size,
      loadFactor: (this._size / this._capacity).toFixed(3),
      maxDisplacement: this._maxDisplacement,
      avgProbesPerLookup: this.stats.lookups > 0 ? (this.stats.probes / this.stats.lookups).toFixed(2) : '0',
    };
  }
}
