// count-min-sketch.js — Count-Min Sketch for approximate frequency estimation
// A probabilistic data structure that uses multiple hash functions and a 2D array
// of counters to estimate the frequency of elements in a stream.
// Space: O(w * d) where w = width (determines error), d = depth (determines confidence)
// Error: ε = e/w, Confidence: 1 - δ = 1 - e^(-d)

/**
 * CountMinSketch — approximate frequency estimation.
 */
export class CountMinSketch {
  constructor(width = 1024, depth = 5) {
    this.width = width;
    this.depth = depth;
    this._table = [];
    for (let d = 0; d < depth; d++) {
      this._table.push(new Int32Array(width));
    }
    this._totalCount = 0;
    this._seeds = Array.from({ length: depth }, (_, i) => i * 0x9e3779b9 + 0x12345);
  }

  /**
   * Add an element (increment its count).
   */
  add(element, count = 1) {
    for (let d = 0; d < this.depth; d++) {
      const pos = this._hash(element, d);
      this._table[d][pos] += count;
    }
    this._totalCount += count;
  }

  /**
   * Estimate the count of an element.
   * Returns the minimum across all hash functions (conservative estimate).
   */
  estimate(element) {
    let min = Infinity;
    for (let d = 0; d < this.depth; d++) {
      const pos = this._hash(element, d);
      if (this._table[d][pos] < min) min = this._table[d][pos];
    }
    return min;
  }

  /**
   * Merge another sketch into this one (additive).
   * Useful for distributed counting.
   */
  merge(other) {
    if (other.width !== this.width || other.depth !== this.depth) {
      throw new Error('Sketches must have same dimensions to merge');
    }
    for (let d = 0; d < this.depth; d++) {
      for (let i = 0; i < this.width; i++) {
        this._table[d][i] += other._table[d][i];
      }
    }
    this._totalCount += other._totalCount;
  }

  /**
   * Point query: what fraction of the stream is element?
   */
  estimateFrequency(element) {
    return this._totalCount > 0 ? this.estimate(element) / this._totalCount : 0;
  }

  /**
   * Heavy hitters: find elements with estimated count above threshold.
   * Requires maintaining a separate set of candidates.
   */
  get totalCount() { return this._totalCount; }

  _hash(element, depth) {
    let h = this._seeds[depth];
    const s = String(element);
    for (let i = 0; i < s.length; i++) {
      h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    }
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    return (((h >>> 16) ^ h) >>> 0) % this.width;
  }

  getStats() {
    return {
      width: this.width,
      depth: this.depth,
      totalCount: this._totalCount,
      memoryBytes: this.width * this.depth * 4, // Int32 = 4 bytes
    };
  }
}
