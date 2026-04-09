// count-min-sketch.js — Approximate frequency counting
//
// A Count-Min Sketch (CMS) estimates the frequency of elements in a data stream
// using sub-linear space. It overestimates (never underestimates) counts.
//
// Used in databases for:
//   - Approximate GROUP BY without full hash table
//   - Hot key detection
//   - Selectivity estimation
//   - Heavy hitter detection (top-k frequent values)
//
// Parameters:
//   - width (w): number of counters per row
//   - depth (d): number of hash functions/rows
//   - ε (epsilon): additive error = 2*n/w where n = total count
//   - δ (delta): probability of error > ε = e^(-d)

function hashFn(key, seed) {
  const str = typeof key === 'string' ? key : String(key);
  let hash = seed;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash ^ str.charCodeAt(i)) * 0x5bd1e995) >>> 0;
    hash ^= hash >>> 13;
  }
  return hash;
}

/**
 * CountMinSketch — Approximate frequency counter.
 */
export class CountMinSketch {
  /**
   * @param {number} width - Counters per row (higher = more accurate)
   * @param {number} depth - Number of hash functions (higher = more confident)
   */
  constructor(width = 1024, depth = 5) {
    this.width = width;
    this.depth = depth;
    this._table = Array.from({ length: depth }, () => new Int32Array(width));
    this._seeds = Array.from({ length: depth }, (_, i) => 0x811c9dc5 + i * 0x01000193);
    this._totalCount = 0;
  }

  /**
   * Create with desired error bounds.
   * @param {number} epsilon - Error bound: actual ≤ estimate ≤ actual + ε*n
   * @param {number} delta - Probability of exceeding error bound
   */
  static withErrorBounds(epsilon = 0.001, delta = 0.01) {
    const width = Math.ceil(Math.E / epsilon);
    const depth = Math.ceil(Math.log(1 / delta));
    return new CountMinSketch(width, depth);
  }

  /**
   * Increment the count for a key.
   */
  add(key, count = 1) {
    for (let i = 0; i < this.depth; i++) {
      const pos = ((hashFn(key, this._seeds[i]) >>> 0) % this.width);
      this._table[i][pos] += count;
    }
    this._totalCount += count;
  }

  /**
   * Estimate the count for a key.
   * Returns the minimum count across all hash functions (most accurate estimate).
   * Guaranteed: actual_count ≤ estimate
   */
  estimate(key) {
    let min = Infinity;
    for (let i = 0; i < this.depth; i++) {
      const pos = ((hashFn(key, this._seeds[i]) >>> 0) % this.width);
      min = Math.min(min, this._table[i][pos]);
    }
    return min;
  }

  /**
   * Total count of all elements added.
   */
  get totalCount() { return this._totalCount; }

  /**
   * Merge two sketches (add their counters).
   * Both must have the same width and depth.
   */
  merge(other) {
    if (this.width !== other.width || this.depth !== other.depth) {
      throw new Error('Cannot merge sketches with different dimensions');
    }
    const merged = new CountMinSketch(this.width, this.depth);
    for (let i = 0; i < this.depth; i++) {
      for (let j = 0; j < this.width; j++) {
        merged._table[i][j] = this._table[i][j] + other._table[i][j];
      }
    }
    merged._totalCount = this._totalCount + other._totalCount;
    merged._seeds = [...this._seeds]; // Same hash functions
    return merged;
  }

  /**
   * Reset all counters.
   */
  clear() {
    for (const row of this._table) row.fill(0);
    this._totalCount = 0;
  }

  /**
   * Get statistics.
   */
  getStats() {
    return {
      width: this.width,
      depth: this.depth,
      totalCount: this._totalCount,
      bytesUsed: this.width * this.depth * 4, // Int32Array
      epsilon: 2 / this.width,     // Error bound per element
      delta: Math.exp(-this.depth), // Error probability
    };
  }
}
