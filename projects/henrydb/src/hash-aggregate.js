// hash-aggregate.js — Hash-based GROUP BY aggregation
export class HashAggregate {
  constructor(groupCols, aggregates) {
    this._groupCols = groupCols;
    this._aggregates = aggregates; // [{col, fn: 'sum'|'count'|'avg'|'min'|'max'}]
    this._groups = new Map();
  }

  addRow(row) {
    const key = this._groupCols.map(c => row[c]).join('|');
    if (!this._groups.has(key)) {
      this._groups.set(key, { key: this._groupCols.reduce((o, c) => { o[c] = row[c]; return o; }, {}), values: [] });
    }
    this._groups.get(key).values.push(row);
  }

  getResults() {
    return [...this._groups.values()].map(g => {
      const result = { ...g.key };
      for (const agg of this._aggregates) {
        const vals = g.values.map(r => r[agg.col]);
        if (agg.fn === 'sum') result[`${agg.fn}_${agg.col}`] = vals.reduce((a, b) => a + b, 0);
        else if (agg.fn === 'count') result[`${agg.fn}_${agg.col}`] = vals.length;
        else if (agg.fn === 'avg') result[`${agg.fn}_${agg.col}`] = vals.reduce((a, b) => a + b, 0) / vals.length;
        else if (agg.fn === 'min') result[`${agg.fn}_${agg.col}`] = Math.min(...vals);
        else if (agg.fn === 'max') result[`${agg.fn}_${agg.col}`] = Math.max(...vals);
      }
      return result;
    });
  }
}
