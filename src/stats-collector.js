// stats-collector.js — Table statistics for cost-based optimizer
// Collects: row count, column NDV (number of distinct values),
// null fraction, min/max, equi-height histograms.
// Used by the query planner to estimate selectivity and costs.

/**
 * ColumnStats — statistics for a single column.
 */
export class ColumnStats {
  constructor(name) {
    this.name = name;
    this.rowCount = 0;
    this.nullCount = 0;
    this.distinctValues = 0;
    this.min = null;
    this.max = null;
    this.histogram = null; // { buckets: [{lo, hi, count, ndv}] }
    this.mostCommonValues = null; // [{value, frequency}]
  }

  get nullFraction() {
    return this.rowCount > 0 ? this.nullCount / this.rowCount : 0;
  }

  /**
   * Estimate selectivity for equality predicate (col = value).
   */
  selectivityEq(value) {
    // Check MCV list first
    if (this.mostCommonValues) {
      const mcv = this.mostCommonValues.find(m => m.value === value);
      if (mcv) return mcv.frequency;
    }
    // Uniform assumption
    return this.distinctValues > 0 ? 1 / this.distinctValues : 0;
  }

  /**
   * Estimate selectivity for range predicate (col > value).
   */
  selectivityGt(value) {
    if (this.min === null || this.max === null) return 0.33; // Default
    if (value >= this.max) return 0;
    if (value < this.min) return 1;
    // Linear interpolation
    return (this.max - value) / (this.max - this.min);
  }

  /**
   * Estimate selectivity for range predicate (col BETWEEN lo AND hi).
   */
  selectivityBetween(lo, hi) {
    if (this.min === null || this.max === null) return 0.25;
    const range = this.max - this.min;
    if (range === 0) return lo <= this.min && hi >= this.max ? 1 : 0;
    const effectiveLo = Math.max(lo, this.min);
    const effectiveHi = Math.min(hi, this.max);
    if (effectiveLo > effectiveHi) return 0;
    return (effectiveHi - effectiveLo) / range;
  }
}

/**
 * StatsCollector — analyze table data and collect statistics.
 */
export class StatsCollector {
  constructor() {
    this._tableStats = new Map();
  }

  /**
   * Analyze a table's data and collect statistics.
   * 
   * @param {string} tableName
   * @param {Array<Object>} rows
   * @param {Object} options - { histogramBuckets: 10, mcvCount: 10, sampleRate: 1.0 }
   */
  analyze(tableName, rows, options = {}) {
    const { histogramBuckets = 10, mcvCount = 10, sampleRate = 1.0 } = options;
    
    // Sample if needed
    let sampleRows = rows;
    if (sampleRate < 1.0) {
      sampleRows = rows.filter(() => Math.random() < sampleRate);
    }

    if (sampleRows.length === 0) return;

    const columns = Object.keys(sampleRows[0]);
    const stats = { rowCount: rows.length, columns: {} };

    for (const col of columns) {
      const cs = new ColumnStats(col);
      cs.rowCount = rows.length;

      const values = sampleRows.map(r => r[col]);
      const nonNull = values.filter(v => v != null);
      cs.nullCount = Math.round((values.length - nonNull.length) * (rows.length / sampleRows.length));

      // Distinct values
      const distinctSet = new Set(nonNull);
      cs.distinctValues = distinctSet.size;

      // Min/Max (for numeric/string)
      if (nonNull.length > 0) {
        const sorted = [...nonNull].sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
        cs.min = sorted[0];
        cs.max = sorted[sorted.length - 1];

        // Most Common Values
        const freq = new Map();
        for (const v of nonNull) freq.set(v, (freq.get(v) || 0) + 1);
        cs.mostCommonValues = [...freq.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, mcvCount)
          .map(([value, count]) => ({ value, frequency: count / rows.length }));

        // Equi-height histogram (for numeric columns)
        if (typeof sorted[0] === 'number') {
          cs.histogram = this._buildHistogram(sorted, histogramBuckets);
        }
      }

      stats.columns[col] = cs;
    }

    this._tableStats.set(tableName, stats);
    return stats;
  }

  /**
   * Get stats for a table.
   */
  getTableStats(tableName) {
    return this._tableStats.get(tableName);
  }

  /**
   * Get column stats.
   */
  getColumnStats(tableName, columnName) {
    const ts = this._tableStats.get(tableName);
    return ts ? ts.columns[columnName] : null;
  }

  /**
   * Estimate the number of rows matching a predicate.
   */
  estimateRows(tableName, predicate) {
    const ts = this._tableStats.get(tableName);
    if (!ts) return null;

    const selectivity = this._estimateSelectivity(ts, predicate);
    return Math.round(ts.rowCount * selectivity);
  }

  _estimateSelectivity(tableStats, pred) {
    if (!pred) return 1;

    const cs = tableStats.columns[pred.column];
    if (!cs) return 0.33; // Unknown column

    switch (pred.op) {
      case 'EQ': return cs.selectivityEq(pred.value);
      case 'GT': return cs.selectivityGt(pred.value);
      case 'LT': return 1 - cs.selectivityGt(pred.value);
      case 'BETWEEN': return cs.selectivityBetween(pred.low, pred.high);
      case 'IS_NULL': return cs.nullFraction;
      case 'IS_NOT_NULL': return 1 - cs.nullFraction;
      default: return 0.33;
    }
  }

  _buildHistogram(sortedValues, numBuckets) {
    const n = sortedValues.length;
    const bucketSize = Math.ceil(n / numBuckets);
    const buckets = [];

    for (let i = 0; i < numBuckets; i++) {
      const start = i * bucketSize;
      const end = Math.min(start + bucketSize, n);
      if (start >= n) break;

      const slice = sortedValues.slice(start, end);
      buckets.push({
        lo: slice[0],
        hi: slice[slice.length - 1],
        count: slice.length,
        ndv: new Set(slice).size,
      });
    }

    return { buckets };
  }
}
