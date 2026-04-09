// robin-hood-hash.js — Robin Hood hashing: open-addressing hash table
//
// Robin Hood hashing is an open-addressing hash table that reduces
// variance in probe lengths by "robbing from the rich" — when inserting,
// if the current element has a shorter probe distance than the element
// at the current slot, they swap (the "richer" element gives up its spot).
//
// Properties:
//   - Expected max probe length: O(log log n) (much better than linear probing)
//   - Low variance: all elements have similar probe distances
//   - Cache-friendly: sequential memory access
//   - Good for hash joins in databases
//
// Used by: Rust's std::HashMap (until 1.36), many game engines, some DB systems

const EMPTY = Symbol('EMPTY');
const DELETED = Symbol('DELETED');

/**
 * RobinHoodHashMap — Open-addressing hash table with Robin Hood probing.
 */
export class RobinHoodHashMap {
  /**
   * @param {number} initialCapacity - Initial table size (will be rounded to power of 2)
   * @param {number} maxLoadFactor - Maximum load before resize (default: 0.85)
   */
  constructor(initialCapacity = 16, maxLoadFactor = 0.85) {
    this._capacity = 1 << Math.ceil(Math.log2(Math.max(4, initialCapacity)));
    this._mask = this._capacity - 1;
    this._maxLoad = maxLoadFactor;
    
    this._keys = new Array(this._capacity).fill(EMPTY);
    this._values = new Array(this._capacity).fill(EMPTY);
    this._distances = new Int32Array(this._capacity); // Probe distance for each slot
    this._size = 0;
    this._maxProbe = 0;
    this._totalProbes = 0; // For average probe length
  }

  get size() { return this._size; }
  get capacity() { return this._capacity; }
  get loadFactor() { return this._size / this._capacity; }

  /**
   * Hash a key to an index.
   */
  _hash(key) {
    const str = typeof key === 'string' ? key : String(key);
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = (h * 0x01000193) >>> 0;
    }
    // Additional mixing
    h ^= h >>> 16;
    h = (h * 0x85ebca6b) >>> 0;
    h ^= h >>> 13;
    return h & this._mask;
  }

  /**
   * Insert or update a key-value pair.
   */
  set(key, value) {
    if (this._size >= this._capacity * this._maxLoad) {
      this._resize(this._capacity * 2);
    }
    
    this._insert(key, value);
  }

  _insert(key, value) {
    let idx = this._hash(key);
    let dist = 0;
    
    // Track the element we're trying to insert
    let insertKey = key;
    let insertValue = value;
    let insertDist = dist;
    
    while (true) {
      const slotKey = this._keys[idx];
      
      // Empty slot — insert here
      if (slotKey === EMPTY || slotKey === DELETED) {
        this._keys[idx] = insertKey;
        this._values[idx] = insertValue;
        this._distances[idx] = insertDist;
        this._size++;
        this._maxProbe = Math.max(this._maxProbe, insertDist);
        return;
      }
      
      // Key already exists — update
      if (slotKey === key) {
        this._values[idx] = value;
        return;
      }
      
      // Robin Hood: if our probe distance is longer, steal the slot
      if (insertDist > this._distances[idx]) {
        // Swap with existing element
        const tmpKey = this._keys[idx];
        const tmpValue = this._values[idx];
        const tmpDist = this._distances[idx];
        
        this._keys[idx] = insertKey;
        this._values[idx] = insertValue;
        this._distances[idx] = insertDist;
        
        insertKey = tmpKey;
        insertValue = tmpValue;
        insertDist = tmpDist;
      }
      
      // Continue probing
      idx = (idx + 1) & this._mask;
      insertDist++;
    }
  }

  /**
   * Get a value by key. O(1) expected.
   */
  get(key) {
    let idx = this._hash(key);
    let dist = 0;
    
    while (true) {
      const slotKey = this._keys[idx];
      
      if (slotKey === EMPTY) return undefined;
      if (slotKey === key) return this._values[idx];
      
      // Robin Hood optimization: if current probe distance > slot's distance,
      // the key definitely doesn't exist (it would have been placed here)
      if (dist > this._distances[idx]) return undefined;
      
      idx = (idx + 1) & this._mask;
      dist++;
    }
  }

  /**
   * Check if a key exists.
   */
  has(key) {
    return this.get(key) !== undefined;
  }

  /**
   * Delete a key. Uses backward shift deletion.
   */
  delete(key) {
    let idx = this._hash(key);
    let dist = 0;
    
    while (true) {
      if (this._keys[idx] === EMPTY) return false;
      if (dist > this._distances[idx]) return false;
      
      if (this._keys[idx] === key) {
        // Found — backward shift to fill gap
        this._backwardShiftDelete(idx);
        this._size--;
        return true;
      }
      
      idx = (idx + 1) & this._mask;
      dist++;
    }
  }

  _backwardShiftDelete(idx) {
    let current = idx;
    let next = (current + 1) & this._mask;
    
    while (this._keys[next] !== EMPTY && this._distances[next] > 0) {
      // Shift back
      this._keys[current] = this._keys[next];
      this._values[current] = this._values[next];
      this._distances[current] = this._distances[next] - 1;
      
      current = next;
      next = (current + 1) & this._mask;
    }
    
    // Clear the last slot
    this._keys[current] = EMPTY;
    this._values[current] = EMPTY;
    this._distances[current] = 0;
  }

  /**
   * Resize the table.
   */
  _resize(newCapacity) {
    const oldKeys = this._keys;
    const oldValues = this._values;
    const oldCapacity = this._capacity;
    
    this._capacity = newCapacity;
    this._mask = newCapacity - 1;
    this._keys = new Array(newCapacity).fill(EMPTY);
    this._values = new Array(newCapacity).fill(EMPTY);
    this._distances = new Int32Array(newCapacity);
    this._size = 0;
    this._maxProbe = 0;
    
    for (let i = 0; i < oldCapacity; i++) {
      if (oldKeys[i] !== EMPTY && oldKeys[i] !== DELETED) {
        this._insert(oldKeys[i], oldValues[i]);
      }
    }
  }

  /**
   * Iterate all entries.
   */
  *entries() {
    for (let i = 0; i < this._capacity; i++) {
      if (this._keys[i] !== EMPTY && this._keys[i] !== DELETED) {
        yield { key: this._keys[i], value: this._values[i], probeDistance: this._distances[i] };
      }
    }
  }

  /**
   * Get statistics about probe distances.
   */
  getStats() {
    let totalDist = 0;
    let maxDist = 0;
    let distHisto = {};
    
    for (let i = 0; i < this._capacity; i++) {
      if (this._keys[i] !== EMPTY && this._keys[i] !== DELETED) {
        const d = this._distances[i];
        totalDist += d;
        maxDist = Math.max(maxDist, d);
        distHisto[d] = (distHisto[d] || 0) + 1;
      }
    }
    
    return {
      size: this._size,
      capacity: this._capacity,
      loadFactor: parseFloat(this.loadFactor.toFixed(4)),
      avgProbeDistance: this._size > 0 ? parseFloat((totalDist / this._size).toFixed(2)) : 0,
      maxProbeDistance: maxDist,
      probeDistribution: distHisto,
    };
  }
}
