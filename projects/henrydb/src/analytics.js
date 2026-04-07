// analytics.js — T-Digest + Segment Tree for HenryDB analytics

/**
 * Simplified T-Digest for quantile estimation.
 * Approximates percentiles (P50, P95, P99) from streaming data.
 * Based on the T-Digest paper by Ted Dunning.
 */
export class TDigest {
  constructor(compression = 100) {
    this._compression = compression;
    this._centroids = []; // { mean, count }
    this._totalCount = 0;
    this._min = Infinity;
    this._max = -Infinity;
  }

  /**
   * Add a value to the digest.
   */
  add(value, count = 1) {
    this._totalCount += count;
    this._min = Math.min(this._min, value);
    this._max = Math.max(this._max, value);
    
    // Find nearest centroid
    let nearestIdx = -1;
    let nearestDist = Infinity;
    for (let i = 0; i < this._centroids.length; i++) {
      const dist = Math.abs(this._centroids[i].mean - value);
      if (dist < nearestDist) {
        nearestDist = dist;
        nearestIdx = i;
      }
    }
    
    if (nearestIdx >= 0 && this._centroids[nearestIdx].count < this._compression / 2) {
      // Merge into nearest centroid
      const c = this._centroids[nearestIdx];
      c.mean = (c.mean * c.count + value * count) / (c.count + count);
      c.count += count;
    } else {
      // Create new centroid
      this._centroids.push({ mean: value, count });
      this._centroids.sort((a, b) => a.mean - b.mean);
    }
    
    // Compress if too many centroids
    if (this._centroids.length > this._compression * 2) {
      this._compress();
    }
  }

  /**
   * Estimate the value at a given quantile (0-1).
   * e.g., quantile(0.5) = median, quantile(0.95) = P95
   */
  quantile(q) {
    if (this._centroids.length === 0) return NaN;
    if (q <= 0) return this._min;
    if (q >= 1) return this._max;
    
    const target = q * this._totalCount;
    let cumulative = 0;
    
    for (let i = 0; i < this._centroids.length; i++) {
      if (cumulative + this._centroids[i].count > target) {
        // Interpolate within this centroid
        if (i === 0) return this._centroids[0].mean;
        const prev = this._centroids[i - 1];
        const curr = this._centroids[i];
        const frac = (target - cumulative) / curr.count;
        return prev.mean + (curr.mean - prev.mean) * frac;
      }
      cumulative += this._centroids[i].count;
    }
    
    return this._centroids[this._centroids.length - 1].mean;
  }

  /**
   * Common percentiles.
   */
  p50() { return this.quantile(0.5); }
  p90() { return this.quantile(0.9); }
  p95() { return this.quantile(0.95); }
  p99() { return this.quantile(0.99); }

  get count() { return this._totalCount; }
  get min() { return this._min; }
  get max() { return this._max; }

  _compress() {
    // Merge adjacent centroids
    const merged = [];
    for (const c of this._centroids) {
      if (merged.length > 0 && merged[merged.length - 1].count < this._compression / 4) {
        const last = merged[merged.length - 1];
        last.mean = (last.mean * last.count + c.mean * c.count) / (last.count + c.count);
        last.count += c.count;
      } else {
        merged.push({ ...c });
      }
    }
    this._centroids = merged;
  }
}

/**
 * Segment Tree for range queries (min, max, sum).
 * O(n) build, O(log n) query, O(log n) update.
 */
export class SegmentTree {
  constructor(data, operation = 'sum') {
    this._n = data.length;
    this._op = operation;
    this._tree = new Array(4 * this._n).fill(0);
    this._identity = operation === 'sum' ? 0 : operation === 'min' ? Infinity : -Infinity;
    
    if (data.length > 0) {
      this._build(data, 1, 0, this._n - 1);
    }
  }

  /**
   * Query aggregate over range [left, right].
   */
  query(left, right) {
    if (this._n === 0) return this._identity;
    return this._query(1, 0, this._n - 1, left, right);
  }

  /**
   * Update value at index.
   */
  update(index, value) {
    this._update(1, 0, this._n - 1, index, value);
  }

  _combine(a, b) {
    if (this._op === 'sum') return a + b;
    if (this._op === 'min') return Math.min(a, b);
    if (this._op === 'max') return Math.max(a, b);
    return a + b;
  }

  _build(data, node, start, end) {
    if (start === end) {
      this._tree[node] = data[start];
      return;
    }
    const mid = (start + end) >>> 1;
    this._build(data, 2 * node, start, mid);
    this._build(data, 2 * node + 1, mid + 1, end);
    this._tree[node] = this._combine(this._tree[2 * node], this._tree[2 * node + 1]);
  }

  _query(node, start, end, left, right) {
    if (right < start || left > end) return this._identity;
    if (left <= start && end <= right) return this._tree[node];
    const mid = (start + end) >>> 1;
    return this._combine(
      this._query(2 * node, start, mid, left, right),
      this._query(2 * node + 1, mid + 1, end, left, right)
    );
  }

  _update(node, start, end, index, value) {
    if (start === end) {
      this._tree[node] = value;
      return;
    }
    const mid = (start + end) >>> 1;
    if (index <= mid) this._update(2 * node, start, mid, index, value);
    else this._update(2 * node + 1, mid + 1, end, index, value);
    this._tree[node] = this._combine(this._tree[2 * node], this._tree[2 * node + 1]);
  }
}
