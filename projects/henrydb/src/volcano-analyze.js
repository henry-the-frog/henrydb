/**
 * EXPLAIN ANALYZE wrapper for Volcano iterators.
 * Wraps each operator to track timing and row counts.
 */

export class AnalyzeIterator {
  constructor(inner, name) {
    this._inner = inner;
    this._name = name || inner.constructor.name;
    this._rowCount = 0;
    this._openTimeMs = 0;
    this._nextTimeMs = 0;
    this._closeTimeMs = 0;
  }

  open() {
    const start = performance.now();
    this._inner.open();
    this._openTimeMs = performance.now() - start;
  }

  next() {
    const start = performance.now();
    const row = this._inner.next();
    this._nextTimeMs += performance.now() - start;
    if (row !== null) this._rowCount++;
    return row;
  }

  close() {
    const start = performance.now();
    this._inner.close();
    this._closeTimeMs = performance.now() - start;
  }

  explain() {
    return `${this._name} (rows=${this._rowCount}, time=${(this._openTimeMs + this._nextTimeMs + this._closeTimeMs).toFixed(1)}ms)`;
  }

  stats() {
    return {
      operator: this._name,
      rows: this._rowCount,
      openMs: this._openTimeMs,
      nextMs: this._nextTimeMs,
      closeMs: this._closeTimeMs,
      totalMs: this._openTimeMs + this._nextTimeMs + this._closeTimeMs
    };
  }
}

/**
 * Run EXPLAIN ANALYZE on a Volcano plan.
 * @param {Iterator} plan - Volcano iterator tree
 * @returns {{ rows: Array, stats: Object, totalMs: number }}
 */
export function explainAnalyze(plan) {
  const analyzer = new AnalyzeIterator(plan);
  const startTotal = performance.now();
  
  analyzer.open();
  const rows = [];
  let row;
  while ((row = analyzer.next()) !== null) {
    rows.push(row);
  }
  analyzer.close();
  
  const totalMs = performance.now() - startTotal;
  return {
    rows,
    plan: analyzer.explain(),
    stats: analyzer.stats(),
    totalMs
  };
}
