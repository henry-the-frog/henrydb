// timeseries.js — Time-series storage engine for HenryDB
// Optimized for append-only temporal data with time-range queries.

/**
 * Time-Series Store: optimized for timestamp-ordered data.
 * Features: append-only insert, time-range queries, downsampling, retention.
 */
export class TimeSeriesStore {
  constructor(retentionMs = Infinity) {
    this._series = new Map(); // metric → sorted array of { timestamp, value, tags }
    this._retentionMs = retentionMs;
  }

  /**
   * Write a data point.
   */
  write(metric, timestamp, value, tags = {}) {
    if (!this._series.has(metric)) this._series.set(metric, []);
    const points = this._series.get(metric);
    points.push({ timestamp, value, tags });
    // Keep sorted (append-only should mostly be in order)
    if (points.length > 1 && points[points.length - 2].timestamp > timestamp) {
      points.sort((a, b) => a.timestamp - b.timestamp);
    }
  }

  /**
   * Query data points in a time range.
   */
  query(metric, startTime, endTime, tagFilter = null) {
    const points = this._series.get(metric) || [];
    let result = points.filter(p => p.timestamp >= startTime && p.timestamp <= endTime);
    
    if (tagFilter) {
      result = result.filter(p => {
        for (const [key, val] of Object.entries(tagFilter)) {
          if (p.tags[key] !== val) return false;
        }
        return true;
      });
    }
    return result;
  }

  /**
   * Downsample: aggregate points into buckets.
   * @param {string} metric - Metric name
   * @param {number} startTime - Start timestamp
   * @param {number} endTime - End timestamp
   * @param {number} bucketMs - Bucket size in milliseconds
   * @param {string} aggFunc - 'avg', 'sum', 'min', 'max', 'count'
   */
  downsample(metric, startTime, endTime, bucketMs, aggFunc = 'avg') {
    const points = this.query(metric, startTime, endTime);
    const buckets = new Map();
    
    for (const p of points) {
      const bucketKey = Math.floor((p.timestamp - startTime) / bucketMs);
      if (!buckets.has(bucketKey)) buckets.set(bucketKey, []);
      buckets.get(bucketKey).push(p.value);
    }
    
    const result = [];
    for (const [key, values] of [...buckets.entries()].sort((a, b) => a[0] - b[0])) {
      const timestamp = startTime + key * bucketMs;
      let value;
      switch (aggFunc) {
        case 'avg': value = values.reduce((a, b) => a + b, 0) / values.length; break;
        case 'sum': value = values.reduce((a, b) => a + b, 0); break;
        case 'min': value = Math.min(...values); break;
        case 'max': value = Math.max(...values); break;
        case 'count': value = values.length; break;
        default: value = values.reduce((a, b) => a + b, 0) / values.length;
      }
      result.push({ timestamp, value, count: values.length });
    }
    return result;
  }

  /**
   * Get the latest value for a metric.
   */
  latest(metric) {
    const points = this._series.get(metric);
    return points && points.length > 0 ? points[points.length - 1] : null;
  }

  /**
   * Apply retention policy: remove points older than retention period.
   */
  applyRetention() {
    if (this._retentionMs === Infinity) return 0;
    const cutoff = Date.now() - this._retentionMs;
    let removed = 0;
    
    for (const [metric, points] of this._series) {
      const before = points.length;
      const filtered = points.filter(p => p.timestamp >= cutoff);
      this._series.set(metric, filtered);
      removed += before - filtered.length;
    }
    return removed;
  }

  /**
   * List all metrics.
   */
  metrics() {
    const result = [];
    for (const [name, points] of this._series) {
      result.push({
        name,
        pointCount: points.length,
        oldest: points[0]?.timestamp,
        newest: points[points.length - 1]?.timestamp,
      });
    }
    return result;
  }
}
