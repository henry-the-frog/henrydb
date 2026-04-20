// tdigest.js — T-Digest for approximate quantile/percentile estimation
// Maintains a sorted set of "centroids" that adaptively compress the distribution.
// Centroids near the tails (P1, P99) have fewer elements for higher accuracy.
// Based on Dunning & Ertl, "Computing Extremely Accurate Quantiles Using t-Digests"

/**
 * TDigest — approximate percentile estimation.
 */
export class TDigest {
  constructor(compression = 100) {
    this.compression = compression;
    this._centroids = []; // { mean, count }
    this._buffer = [];    // unsorted incoming values
    this._totalCount = 0;
    this._min = Infinity;
    this._max = -Infinity;
    this._needsSort = false;
    this._bufferCapacity = Math.max(500, compression * 5);
  }

  /**
   * Add a value.
   */
  add(value, count = 1) {
    this._buffer.push({ mean: value, count });
    this._totalCount += count;
    if (value < this._min) this._min = value;
    if (value > this._max) this._max = value;

    // Flush buffer when it gets large
    if (this._buffer.length >= this._bufferCapacity) {
      this._flushBuffer();
    }
  }

  _flushBuffer() {
    if (this._buffer.length === 0) return;
    this._centroids.push(...this._buffer);
    this._buffer = [];
    this._needsSort = true;
    this._compress();
  }

  /**
   * Estimate the value at a given quantile (0-1).
   * quantile(0.5) = median, quantile(0.95) = P95
   */
  quantile(q) {
    if (this._totalCount === 0) return null;
    if (q <= 0) return this._min;
    if (q >= 1) return this._max;

    this._flushBuffer();
    this._ensureSorted();

    const target = q * this._totalCount;
    let cumulative = 0;

    for (let i = 0; i < this._centroids.length; i++) {
      const c = this._centroids[i];
      const nextCum = cumulative + c.count;

      if (target <= nextCum) {
        // Interpolate within this centroid
        if (i === 0) return c.mean;
        
        const prevC = this._centroids[i - 1];
        const fraction = (target - cumulative) / c.count;
        return prevC.mean + (c.mean - prevC.mean) * fraction;
      }

      cumulative = nextCum;
    }

    return this._max;
  }

  /**
   * Percentile (convenience): quantile(p/100).
   */
  percentile(p) {
    return this.quantile(p / 100);
  }

  /**
   * Merge another T-Digest into this one.
   */
  merge(other) {
    this._flushBuffer();
    for (const c of other._centroids) {
      this._centroids.push({ ...c });
    }
    // Also merge any buffered values from other
    for (const c of other._buffer) {
      this._centroids.push({ ...c });
    }
    this._totalCount += other._totalCount;
    if (other._min < this._min) this._min = other._min;
    if (other._max > this._max) this._max = other._max;
    this._needsSort = true;
    this._compress();
  }

  /**
   * Compress centroids to maintain bounded size.
   */
  _compress() {
    this._ensureSorted();

    const compressed = [];
    let i = 0;
    let cumCount = 0; // running cumulative count

    while (i < this._centroids.length) {
      let merged = { ...this._centroids[i] };
      i++;

      // Try to merge nearby centroids
      while (i < this._centroids.length) {
        const next = this._centroids[i];
        const newCount = merged.count + next.count;
        
        // Size limit: centroids near the tails should be small
        const qEstimate = (cumCount + merged.count / 2) / this._totalCount;
        const maxSize = this._maxCentroidSize(qEstimate);
        
        if (newCount <= maxSize) {
          // Merge: weighted mean
          merged.mean = (merged.mean * merged.count + next.mean * next.count) / newCount;
          merged.count = newCount;
          i++;
        } else {
          break;
        }
      }

      cumCount += merged.count;
      compressed.push(merged);
    }

    this._centroids = compressed;
    this._needsSort = false;
  }

  _maxCentroidSize(q) {
    // Near tails: small. Near median: large.
    // Using the t-digest scaling function: 4n * q * (1-q) / compression
    return Math.max(1, Math.floor(4 * this._totalCount * q * (1 - q) / this.compression));
  }

  _cumCountBefore(centroids, current) {
    let sum = 0;
    for (const c of centroids) sum += c.count;
    return sum + current.count / 2;
  }

  _ensureSorted() {
    if (this._needsSort) {
      this._centroids.sort((a, b) => a.mean - b.mean);
      this._needsSort = false;
    }
  }

  get count() { return this._totalCount; }
  get min() { return this._min; }
  get max() { return this._max; }
  get centroidCount() { this._flushBuffer(); return this._centroids.length; }

  getStats() {
    return {
      count: this._totalCount,
      centroids: this._centroids.length,
      min: this._min,
      max: this._max,
      compression: this.compression,
    };
  }
}
