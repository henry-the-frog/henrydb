// hash-group-by.js — In-memory hash aggregation
// Build a hash table keyed by GROUP BY columns.
// Each bucket accumulates aggregate state (SUM, COUNT, AVG, MIN, MAX).

export class HashGroupBy {
  constructor(groupCols, aggregates) {
    this.groupCols = groupCols; // ['dept', 'year']
    this.aggregates = aggregates; // [{col, func, alias}]
    this._groups = new Map();
  }

  /** Add a row to the appropriate group */
  addRow(row) {
    const key = this.groupCols.map(c => row[c]).join('|');
    if (!this._groups.has(key)) {
      this._groups.set(key, {
        keyValues: Object.fromEntries(this.groupCols.map(c => [c, row[c]])),
        accumulators: this.aggregates.map(agg => this._initAcc(agg.func)),
      });
    }
    const group = this._groups.get(key);
    for (let i = 0; i < this.aggregates.length; i++) {
      this._accumulate(group.accumulators[i], row[this.aggregates[i].col], this.aggregates[i].func);
    }
  }

  /** Process all rows at once */
  addAll(rows) { for (const row of rows) this.addRow(row); }

  /** Get final results */
  results() {
    const out = [];
    for (const group of this._groups.values()) {
      const row = { ...group.keyValues };
      for (let i = 0; i < this.aggregates.length; i++) {
        row[this.aggregates[i].alias] = this._finalize(group.accumulators[i], this.aggregates[i].func);
      }
      out.push(row);
    }
    return out;
  }

  _initAcc(func) {
    switch (func) {
      case 'SUM': return { sum: 0 };
      case 'COUNT': return { count: 0 };
      case 'AVG': return { sum: 0, count: 0 };
      case 'MIN': return { min: Infinity };
      case 'MAX': return { max: -Infinity };
      case 'COUNT_DISTINCT': return { set: new Set() };
      default: return {};
    }
  }

  _accumulate(acc, value, func) {
    if (value == null) return;
    switch (func) {
      case 'SUM': acc.sum += value; break;
      case 'COUNT': acc.count++; break;
      case 'AVG': acc.sum += value; acc.count++; break;
      case 'MIN': if (value < acc.min) acc.min = value; break;
      case 'MAX': if (value > acc.max) acc.max = value; break;
      case 'COUNT_DISTINCT': acc.set.add(value); break;
    }
  }

  _finalize(acc, func) {
    switch (func) {
      case 'SUM': return acc.sum;
      case 'COUNT': return acc.count;
      case 'AVG': return acc.count > 0 ? acc.sum / acc.count : null;
      case 'MIN': return acc.min === Infinity ? null : acc.min;
      case 'MAX': return acc.max === -Infinity ? null : acc.max;
      case 'COUNT_DISTINCT': return acc.set.size;
      default: return null;
    }
  }

  get groupCount() { return this._groups.size; }
}
