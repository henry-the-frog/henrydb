// predicate.js — Composable predicates for query filtering
export class Predicate {
  constructor(fn) { this._fn = fn; }
  test(row) { return this._fn(row); }
  
  and(other) { return new Predicate(row => this._fn(row) && other._fn(row)); }
  or(other) { return new Predicate(row => this._fn(row) || other._fn(row)); }
  not() { return new Predicate(row => !this._fn(row)); }

  static eq(col, val) { return new Predicate(row => row[col] === val); }
  static gt(col, val) { return new Predicate(row => row[col] > val); }
  static lt(col, val) { return new Predicate(row => row[col] < val); }
  static gte(col, val) { return new Predicate(row => row[col] >= val); }
  static lte(col, val) { return new Predicate(row => row[col] <= val); }
  static between(col, lo, hi) { return new Predicate(row => row[col] >= lo && row[col] <= hi); }
  static like(col, pattern) {
    const re = new RegExp('^' + pattern.replace(/%/g, '.*').replace(/_/g, '.') + '$', 'i');
    return new Predicate(row => re.test(row[col]));
  }
  static isNull(col) { return new Predicate(row => row[col] == null); }
  static in(col, values) { const set = new Set(values); return new Predicate(row => set.has(row[col])); }
  static always() { return new Predicate(() => true); }
  static never() { return new Predicate(() => false); }
}
