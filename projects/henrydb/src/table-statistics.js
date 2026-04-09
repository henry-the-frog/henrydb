// table-statistics.js — Table statistics collector for query optimizer
// ANALYZE command, histograms, cardinality estimation, null fractions.

/**
 * ColumnStatistics — statistics for a single column.
 */
class ColumnStatistics {
  constructor(columnName) {
    this.columnName = columnName;
    this.totalRows = 0;
    this.nullCount = 0;
    this.distinctCount = 0;
    this.minValue = null;
    this.maxValue = null;
    this.avgWidth = 0; // Average byte width
    this.histogram = []; // Equi-depth histogram bounds
    this.mostCommonValues = []; // [{value, frequency}]
    this.correlation = 0; // Physical vs logical ordering correlation
    this.nullFraction = 0;
    this.distinctRatio = 0; // n_distinct / total_rows
  }
}

/**
 * TableStatistics — statistics for an entire table.
 */
export class TableStatistics {
  constructor(tableName) {
    this.tableName = tableName;
    this.totalRows = 0;
    this.totalPages = 0; // Estimated disk pages
    this.columns = new Map(); // column name → ColumnStatistics
    this.lastAnalyzed = null;
    this.analyzeCount = 0;
  }

  getColumn(name) {
    return this.columns.get(name);
  }
}

/**
 * StatisticsCollector — analyzes tables and generates statistics.
 */
export class StatisticsCollector {
  constructor(options = {}) {
    this.histogramBuckets = options.histogramBuckets || 100;
    this.mcvCount = options.mcvCount || 10; // Most common values to track
    this.sampleRatio = options.sampleRatio || 1.0; // 1.0 = full scan, < 1.0 = sampling
    this._stats = new Map(); // table name → TableStatistics
  }

  /**
   * Analyze a table's data and generate statistics.
   * @param {string} tableName
   * @param {object[]} rows - All rows in the table
   * @param {string[]} columns - Column names to analyze
   */
  analyze(tableName, rows, columns) {
    const stats = new TableStatistics(tableName);
    stats.totalRows = rows.length;
    stats.totalPages = Math.ceil(rows.length / 100); // Assume ~100 rows per page
    stats.lastAnalyzed = Date.now();
    stats.analyzeCount = (this._stats.get(tableName)?.analyzeCount || 0) + 1;

    // Sample if needed
    const sampleRows = this.sampleRatio < 1.0
      ? this._sample(rows, Math.ceil(rows.length * this.sampleRatio))
      : rows;

    for (const colName of columns) {
      const colStats = this._analyzeColumn(colName, sampleRows, rows.length);
      stats.columns.set(colName, colStats);
    }

    this._stats.set(tableName, stats);
    return stats;
  }

  /**
   * Get statistics for a table.
   */
  getStats(tableName) {
    return this._stats.get(tableName) || null;
  }

  /**
   * Estimate selectivity of a simple predicate.
   * Returns a fraction 0-1 representing the estimated fraction of rows matching.
   */
  estimateSelectivity(tableName, column, op, value) {
    const tableStats = this._stats.get(tableName);
    if (!tableStats) return 0.5; // No stats, assume 50%

    const colStats = tableStats.columns.get(column);
    if (!colStats) return 0.5;

    switch (op) {
      case '=': {
        // Check most common values first
        const mcv = colStats.mostCommonValues.find(v => v.value === value);
        if (mcv) return mcv.frequency;
        // Use distinct count for uniform assumption
        if (colStats.distinctCount > 0) {
          return 1.0 / colStats.distinctCount;
        }
        return 0.01;
      }

      case '!=':
      case '<>':
        return 1 - this.estimateSelectivity(tableName, column, '=', value);

      case '<': {
        if (colStats.minValue === null || colStats.maxValue === null) return 0.33;
        const range = colStats.maxValue - colStats.minValue;
        if (range === 0) return value > colStats.minValue ? 1.0 : 0.0;
        return Math.max(0, Math.min(1, (value - colStats.minValue) / range));
      }

      case '>': {
        return 1 - this.estimateSelectivity(tableName, column, '<=', value);
      }

      case '<=': {
        if (colStats.minValue === null || colStats.maxValue === null) return 0.33;
        const range = colStats.maxValue - colStats.minValue;
        if (range === 0) return value >= colStats.minValue ? 1.0 : 0.0;
        return Math.max(0, Math.min(1, (value - colStats.minValue + 1) / (range + 1)));
      }

      case '>=': {
        return 1 - this.estimateSelectivity(tableName, column, '<', value);
      }

      case 'IS NULL':
        return colStats.nullFraction;

      case 'IS NOT NULL':
        return 1 - colStats.nullFraction;

      case 'BETWEEN': {
        if (!Array.isArray(value) || value.length < 2) return 0.33;
        const selLow = this.estimateSelectivity(tableName, column, '>=', value[0]);
        const selHigh = this.estimateSelectivity(tableName, column, '<=', value[1]);
        return Math.max(0, selLow + selHigh - 1);
      }

      default:
        return 0.5;
    }
  }

  /**
   * Estimate the number of rows a query will return.
   */
  estimateRowCount(tableName, predicates = []) {
    const tableStats = this._stats.get(tableName);
    if (!tableStats) return 1000; // Default estimate

    let selectivity = 1.0;
    for (const pred of predicates) {
      selectivity *= this.estimateSelectivity(tableName, pred.column, pred.op, pred.value);
    }

    return Math.max(1, Math.round(tableStats.totalRows * selectivity));
  }

  _analyzeColumn(colName, rows, totalRows) {
    const stats = new ColumnStatistics(colName);
    stats.totalRows = totalRows;

    // Collect values
    const values = [];
    const valueCounts = new Map();
    let nullCount = 0;
    let totalWidth = 0;

    for (const row of rows) {
      const val = row[colName];
      if (val === null || val === undefined) {
        nullCount++;
        continue;
      }
      values.push(val);
      valueCounts.set(val, (valueCounts.get(val) || 0) + 1);
      totalWidth += String(val).length;
    }

    stats.nullCount = nullCount;
    stats.nullFraction = totalRows > 0 ? nullCount / totalRows : 0;
    stats.distinctCount = valueCounts.size;
    stats.distinctRatio = totalRows > 0 ? valueCounts.size / totalRows : 0;
    stats.avgWidth = values.length > 0 ? totalWidth / values.length : 0;

    // Min/Max
    if (values.length > 0) {
      const sorted = [...values].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
      stats.minValue = sorted[0];
      stats.maxValue = sorted[sorted.length - 1];

      // Histogram (equi-depth)
      stats.histogram = this._buildHistogram(sorted, this.histogramBuckets);

      // Most common values
      stats.mostCommonValues = [...valueCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, this.mcvCount)
        .map(([value, count]) => ({
          value,
          frequency: count / totalRows,
          count,
        }));

      // Correlation (how well the physical order matches the sort order)
      stats.correlation = this._computeCorrelation(rows.map(r => r[colName]).filter(v => v != null), sorted);
    }

    return stats;
  }

  _buildHistogram(sorted, numBuckets) {
    if (sorted.length === 0) return [];
    const bucketSize = Math.max(1, Math.floor(sorted.length / numBuckets));
    const bounds = [];
    for (let i = 0; i < sorted.length; i += bucketSize) {
      bounds.push(sorted[i]);
    }
    bounds.push(sorted[sorted.length - 1]);
    return bounds;
  }

  _computeCorrelation(original, sorted) {
    if (original.length <= 1) return 1;
    // Simplified: count inversions
    let concordant = 0;
    let total = 0;
    const n = Math.min(original.length, 100); // Sample for large arrays
    for (let i = 0; i < n - 1; i++) {
      if (original[i] <= original[i + 1]) concordant++;
      total++;
    }
    return total > 0 ? (2 * concordant / total) - 1 : 0;
  }

  _sample(rows, n) {
    if (n >= rows.length) return rows;
    const sampled = [];
    const indices = new Set();
    while (indices.size < n) {
      indices.add(Math.floor(Math.random() * rows.length));
    }
    for (const idx of indices) {
      sampled.push(rows[idx]);
    }
    return sampled;
  }
}
