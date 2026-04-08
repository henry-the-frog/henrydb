// cardinality.js — Multi-column distinct cardinality estimation
// Uses sampling and HyperLogLog sketches for fast approximation.

function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return h >>> 0;
}

export class CardinalityEstimator {
  constructor(precision = 12) {
    this.p = precision;
    this.m = 1 << precision;
    this.registers = new Uint8Array(this.m);
    this._count = 0;
  }

  /** Add a value (auto-converts to string) */
  add(value) {
    const hash = fnv1a(String(value));
    const idx = hash & (this.m - 1); // Lower p bits for index
    const w = hash >>> this.p; // Upper bits for counting
    const rho = w === 0 ? (32 - this.p) : (Math.clz32(w) - this.p + 1);
    if (rho > this.registers[idx]) this.registers[idx] = rho;
    this._count++;
  }

  /** Add all values from column */
  addAll(values) { for (const v of values) this.add(v); }

  /** Estimate distinct count */
  estimate() {
    const alpha = this.m === 16 ? 0.673 : this.m === 32 ? 0.697 : this.m === 64 ? 0.709 : 0.7213 / (1 + 1.079 / this.m);
    
    let sum = 0;
    let zeros = 0;
    for (let i = 0; i < this.m; i++) {
      sum += 2 ** (-this.registers[i]);
      if (this.registers[i] === 0) zeros++;
    }
    
    let estimate = alpha * this.m * this.m / sum;
    
    // Small range correction
    if (estimate <= 2.5 * this.m && zeros > 0) {
      estimate = this.m * Math.log(this.m / zeros);
    }
    
    return Math.round(estimate);
  }

  /** Merge two estimators */
  merge(other) {
    const result = new CardinalityEstimator(this.p);
    for (let i = 0; i < this.m; i++) {
      result.registers[i] = Math.max(this.registers[i], other.registers[i]);
    }
    return result;
  }

  /** Estimate error margin (standard error) */
  get standardError() { return 1.04 / Math.sqrt(this.m); }
}

/**
 * Multi-column cardinality estimator using combined hash.
 */
export class MultiColumnEstimator {
  constructor(precision = 12) {
    this.precision = precision;
    this._estimators = new Map(); // column → CardinalityEstimator
    this._combined = new CardinalityEstimator(precision);
  }

  /** Add a row with multiple columns */
  addRow(row, columns) {
    for (const col of columns) {
      if (!this._estimators.has(col)) this._estimators.set(col, new CardinalityEstimator(this.precision));
      this._estimators.get(col).add(row[col]);
    }
    // Combined cardinality
    const combinedKey = columns.map(c => row[c]).join('|');
    this._combined.add(combinedKey);
  }

  /** Get single-column cardinality */
  columnCardinality(col) {
    return this._estimators.get(col)?.estimate() || 0;
  }

  /** Get combined cardinality */
  combinedCardinality() { return this._combined.estimate(); }

  /** Estimate join cardinality between two columns */
  joinCardinality(col1, col2) {
    const c1 = this.columnCardinality(col1);
    const c2 = this.columnCardinality(col2);
    return Math.max(c1, c2) > 0 ? Math.round(1 / Math.max(c1, c2)) : 0;
  }
}
