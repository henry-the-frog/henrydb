// table-stats.js — Table statistics for cost-based query optimization
//
// Collects per-column statistics by scanning the table:
//   - Row count
//   - Distinct values (exact for small tables, HyperLogLog-like for large)
//   - Min/max values
//   - Null fraction
//   - Most common values (MCV) with frequencies
//   - Histogram for range selectivity estimation
//
// Based on PostgreSQL's pg_stats concept.

/**
 * ColumnStats — Statistics for a single column.
 */
export class ColumnStats {
  constructor(name) {
    this.name = name;
    this.rowCount = 0;
    this.nullCount = 0;
    this.distinctCount = 0;
    this.min = null;
    this.max = null;
    this.avgWidth = 0; // Average byte width
    this.mcv = [];     // Most common values: [{value, frequency}]
    this.histogram = []; // Equi-depth histogram bounds
  }

  get nullFraction() {
    return this.rowCount > 0 ? this.nullCount / this.rowCount : 0;
  }

  get selectivity() {
    // Estimated selectivity for equality predicate (1/distinct)
    return this.distinctCount > 0 ? 1 / this.distinctCount : 1;
  }
}

/**
 * TableStats — Aggregated statistics for a table.
 */
export class TableStats {
  constructor(tableName) {
    this.tableName = tableName;
    this.rowCount = 0;
    this.columnStats = new Map(); // colName → ColumnStats
    this.analyzedAt = null;
  }
}

/**
 * Analyze a table and collect statistics.
 * @param {Object} tableInfo - {schema, heap} from db.tables
 * @param {Object} options
 * @param {number} options.mcvLimit - Number of most common values to track (default: 10)
 * @param {number} options.histogramBuckets - Number of histogram buckets (default: 100)
 * @returns {TableStats}
 */
export function analyzeTable(tableInfo, options = {}) {
  const { schema, heap } = tableInfo;
  const mcvLimit = options.mcvLimit || 10;
  const histogramBuckets = options.histogramBuckets || 100;
  
  const stats = new TableStats(heap.name || 'unknown');
  
  // Initialize per-column collectors
  const collectors = schema.map((col, idx) => ({
    name: col.name,
    idx,
    values: [],
    nullCount: 0,
    valueCounts: new Map(), // value → count
  }));
  
  // Scan all rows
  let rowCount = 0;
  for (const { values } of heap.scan()) {
    rowCount++;
    for (const col of collectors) {
      const val = values[col.idx];
      if (val === null || val === undefined) {
        col.nullCount++;
      } else {
        col.values.push(val);
        col.valueCounts.set(val, (col.valueCounts.get(val) || 0) + 1);
      }
    }
  }
  
  stats.rowCount = rowCount;
  
  // Compute per-column stats
  for (const col of collectors) {
    const cs = new ColumnStats(col.name);
    cs.rowCount = rowCount;
    cs.nullCount = col.nullCount;
    cs.distinctCount = col.valueCounts.size;
    
    if (col.values.length > 0) {
      // Min/max (works for numbers and strings)
      const sorted = [...col.values].sort((a, b) => {
        if (typeof a === 'number' && typeof b === 'number') return a - b;
        return String(a).localeCompare(String(b));
      });
      cs.min = sorted[0];
      cs.max = sorted[sorted.length - 1];
      
      // Average width
      cs.avgWidth = col.values.reduce((sum, v) => sum + String(v).length, 0) / col.values.length;
      
      // Most Common Values (MCV)
      const mcvEntries = [...col.valueCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, mcvLimit)
        .map(([value, count]) => ({
          value,
          count,
          frequency: count / rowCount,
        }));
      cs.mcv = mcvEntries;
      
      // Histogram (equi-depth bounds)
      if (sorted.length > histogramBuckets) {
        const step = Math.floor(sorted.length / histogramBuckets);
        cs.histogram = [];
        for (let i = 0; i < histogramBuckets; i++) {
          cs.histogram.push(sorted[i * step]);
        }
        cs.histogram.push(sorted[sorted.length - 1]); // Add max
      } else {
        // Small table: just use all sorted values as histogram
        cs.histogram = sorted;
      }
    }
    
    stats.columnStats.set(col.name, cs);
  }
  
  stats.analyzedAt = new Date();
  return stats;
}

/**
 * Estimate selectivity of a predicate using table statistics.
 */
export function estimateSelectivity(colStats, op, value) {
  if (!colStats || colStats.rowCount === 0) return 0.5; // No stats → assume 50%
  
  switch (op) {
    case '=':
    case 'EQ': {
      // Check MCV first
      const mcvEntry = colStats.mcv.find(e => e.value === value);
      if (mcvEntry) return mcvEntry.frequency;
      // Otherwise use 1/distinct
      return colStats.selectivity;
    }
    
    case '<':
    case 'LT': {
      if (colStats.min === null) return 0.5;
      if (typeof value === 'number' && typeof colStats.min === 'number') {
        const range = colStats.max - colStats.min;
        if (range === 0) return value > colStats.min ? 1 : 0;
        return Math.max(0, Math.min(1, (value - colStats.min) / range));
      }
      // Histogram-based for non-numeric
      const pos = colStats.histogram.findIndex(h => h >= value);
      return pos >= 0 ? pos / colStats.histogram.length : 0.5;
    }
    
    case '>':
    case 'GT':
      return 1 - estimateSelectivity(colStats, 'LT', value);
    
    case '<=':
    case 'LTE':
      return estimateSelectivity(colStats, 'LT', value) + estimateSelectivity(colStats, 'EQ', value);
    
    case '>=':
    case 'GTE':
      return 1 - estimateSelectivity(colStats, 'LT', value);
    
    case '!=':
    case 'NEQ':
      return 1 - estimateSelectivity(colStats, 'EQ', value);
    
    default:
      return 0.5;
  }
}
