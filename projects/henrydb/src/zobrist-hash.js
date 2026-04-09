// zobrist-hash.js — Fast incremental hashing using XOR
// Used in: chess engines (transposition tables), database deduplication,
// change detection, and content addressing.
// Key insight: XOR is its own inverse, so adding/removing elements is O(1).

export class ZobristHash {
  /**
   * @param {number} positions - Number of possible positions
   * @param {number} values - Number of possible values per position
   */
  constructor(positions, values) {
    this._table = Array.from({ length: positions }, () =>
      Array.from({ length: values }, () => this._randomBigint())
    );
    this._hash = 0n;
    this._positions = positions;
    this._values = values;
  }

  get hash() { return this._hash; }

  /** Set position to value. O(1). */
  set(position, value) {
    this._hash ^= this._table[position][value];
  }

  /** Move: change position from oldValue to newValue. O(1). */
  move(position, oldValue, newValue) {
    this._hash ^= this._table[position][oldValue]; // Remove old
    this._hash ^= this._table[position][newValue]; // Add new
  }

  /** Reset hash. */
  reset() { this._hash = 0n; }

  /** Compute full hash from state array. */
  computeFull(state) {
    let hash = 0n;
    for (let i = 0; i < state.length; i++) {
      if (state[i] !== null && state[i] !== undefined) {
        hash ^= this._table[i][state[i]];
      }
    }
    return hash;
  }

  _randomBigint() {
    // Generate random 64-bit BigInt
    const bytes = new Uint8Array(8);
    for (let i = 0; i < 8; i++) bytes[i] = Math.floor(Math.random() * 256);
    let n = 0n;
    for (const b of bytes) n = (n << 8n) | BigInt(b);
    return n;
  }
}

/**
 * Simple Zobrist hash for string/set operations.
 */
export class ZobristSetHash {
  constructor() {
    this._table = new Map();
    this._hash = 0n;
  }

  get hash() { return this._hash; }

  add(element) {
    this._hash ^= this._getHash(element);
  }

  remove(element) {
    this._hash ^= this._getHash(element); // XOR is self-inverse
  }

  _getHash(element) {
    const key = String(element);
    if (!this._table.has(key)) {
      const bytes = new Uint8Array(8);
      for (let i = 0; i < 8; i++) bytes[i] = Math.floor(Math.random() * 256);
      let n = 0n;
      for (const b of bytes) n = (n << 8n) | BigInt(b);
      this._table.set(key, n);
    }
    return this._table.get(key);
  }
}
