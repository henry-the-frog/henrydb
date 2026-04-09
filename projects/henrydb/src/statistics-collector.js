// statistics-collector.js — Automatic table statistics for query optimization
export class StatisticsCollector {
  constructor() { this._stats = new Map(); }

  analyze(tableName, column, values) {
    const n = values.length;
    const distinct = new Set(values).size;
    const sorted = [...values].sort((a, b) => a - b);
    
    this._stats.set(`${tableName}.${column}`, {
      rowCount: n,
      distinctCount: distinct,
      nullCount: values.filter(v => v == null).length,
      min: sorted[0],
      max: sorted[sorted.length - 1],
      avgWidth: typeof values[0] === 'string' ? values.reduce((s, v) => s + String(v).length, 0) / n : 8,
      selectivity: distinct / n,
    });
  }

  get(tableName, column) { return this._stats.get(`${tableName}.${column}`); }

  estimateSelectivity(tableName, column, value) {
    const s = this.get(tableName, column);
    if (!s) return 0.1; // Default
    return 1 / s.distinctCount; // Uniform assumption
  }

  estimateRangeSelectivity(tableName, column, lo, hi) {
    const s = this.get(tableName, column);
    if (!s) return 0.33;
    return (hi - lo) / (s.max - s.min);
  }
}
