// t-digest.js — Streaming quantile estimation
// Estimates percentiles (p50, p95, p99) in bounded memory using centroids.
// Used in: Elasticsearch, Prometheus, database query profiling.
// Accuracy is highest at extremes (p99, p1) where it matters most.

export class TDigest {
  constructor(compression = 100) {
    this._compression = compression;
    this._centroids = []; // {mean, count}
    this._totalCount = 0;
    this._min = Infinity;
    this._max = -Infinity;
  }

  get count() { return this._totalCount; }

  /** Add a value to the digest. */
  add(value, count = 1) {
    this._centroids.push({ mean: value, count });
    this._totalCount += count;
    this._min = Math.min(this._min, value);
    this._max = Math.max(this._max, value);
    
    if (this._centroids.length > this._compression * 10) {
      this._compress();
    }
  }

  /** Estimate the value at a given quantile (0-1). */
  quantile(q) {
    if (this._totalCount === 0) return NaN;
    if (q <= 0) return this._min;
    if (q >= 1) return this._max;
    
    this._compress();
    
    const target = q * this._totalCount;
    let cumulative = 0;
    
    for (let i = 0; i < this._centroids.length; i++) {
      const c = this._centroids[i];
      if (cumulative + c.count >= target) {
        // Interpolate within centroid
        const fraction = (target - cumulative) / c.count;
        if (i === 0) return this._min + (c.mean - this._min) * fraction;
        const prev = this._centroids[i - 1];
        return prev.mean + (c.mean - prev.mean) * fraction;
      }
      cumulative += c.count;
    }
    
    return this._max;
  }

  /** Common percentiles. */
  p50() { return this.quantile(0.5); }
  p90() { return this.quantile(0.9); }
  p95() { return this.quantile(0.95); }
  p99() { return this.quantile(0.99); }

  /** Merge another t-digest into this one. */
  merge(other) {
    for (const c of other._centroids) {
      this._centroids.push({ ...c });
      this._totalCount += c.count;
    }
    this._min = Math.min(this._min, other._min);
    this._max = Math.max(this._max, other._max);
    this._compress();
  }

  _compress() {
    this._centroids.sort((a, b) => a.mean - b.mean);
    
    const merged = [];
    let current = { ...this._centroids[0] };
    
    for (let i = 1; i < this._centroids.length; i++) {
      const c = this._centroids[i];
      // Weight limit: tighter at extremes (q near 0 or 1)
      const q = (current.count / 2 + c.count / 2) / this._totalCount;
      const maxSize = 4 * this._compression * q * (1 - q);
      
      if (current.count + c.count <= Math.max(1, maxSize)) {
        // Merge centroids
        const total = current.count + c.count;
        current.mean = (current.mean * current.count + c.mean * c.count) / total;
        current.count = total;
      } else {
        merged.push(current);
        current = { ...c };
      }
    }
    merged.push(current);
    this._centroids = merged;
  }

  getStats() {
    return {
      count: this._totalCount,
      centroids: this._centroids.length,
      min: this._min,
      max: this._max,
      compression: this._compression,
    };
  }
}
