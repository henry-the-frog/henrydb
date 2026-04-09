// range-types.js — PostgreSQL-compatible range types for HenryDB
// int4range, numrange, tsrange, daterange + operators.

/**
 * Range — a range of values with inclusive/exclusive bounds.
 */
export class Range {
  constructor(lower, upper, options = {}) {
    this.lower = lower;
    this.upper = upper;
    this.lowerInc = options.lowerInc !== false; // Default inclusive
    this.upperInc = options.upperInc || false; // Default exclusive
    this.empty = options.empty || false;
  }

  static empty() {
    return new Range(null, null, { empty: true });
  }

  static fromString(str, parser = Number) {
    if (str === 'empty') return Range.empty();
    const lowerInc = str[0] === '[';
    const upperInc = str[str.length - 1] === ']';
    const inner = str.slice(1, -1);
    const [lStr, uStr] = inner.split(',').map(s => s.trim());
    const lower = lStr === '' ? null : parser(lStr);
    const upper = uStr === '' ? null : parser(uStr);
    return new Range(lower, upper, { lowerInc, upperInc });
  }

  /**
   * Check if range contains a value.
   */
  contains(value) {
    if (this.empty) return false;
    if (this.lower !== null) {
      if (this.lowerInc ? value < this.lower : value <= this.lower) return false;
    }
    if (this.upper !== null) {
      if (this.upperInc ? value > this.upper : value >= this.upper) return false;
    }
    return true;
  }

  /**
   * Check if this range contains another range (@>).
   */
  containsRange(other) {
    if (other.empty) return true;
    if (this.empty) return false;

    const lowerOk = this.lower === null || (
      other.lower !== null && (
        this.lower < other.lower ||
        (this.lower === other.lower && (this.lowerInc || !other.lowerInc))
      )
    );
    const upperOk = this.upper === null || (
      other.upper !== null && (
        this.upper > other.upper ||
        (this.upper === other.upper && (this.upperInc || !other.upperInc))
      )
    );
    return lowerOk && upperOk;
  }

  /**
   * Check if ranges overlap (&&).
   */
  overlaps(other) {
    if (this.empty || other.empty) return false;

    const thisLower = this.lower ?? -Infinity;
    const thisUpper = this.upper ?? Infinity;
    const otherLower = other.lower ?? -Infinity;
    const otherUpper = other.upper ?? Infinity;

    if (thisLower > otherUpper || otherLower > thisUpper) return false;
    if (thisLower === otherUpper && !(this.lowerInc && other.upperInc)) return false;
    if (otherLower === thisUpper && !(other.lowerInc && this.upperInc)) return false;

    return true;
  }

  /**
   * Intersection of two ranges (*).
   */
  intersection(other) {
    if (!this.overlaps(other)) return Range.empty();

    let lower, lowerInc;
    if (this.lower === null) { lower = other.lower; lowerInc = other.lowerInc; }
    else if (other.lower === null) { lower = this.lower; lowerInc = this.lowerInc; }
    else if (this.lower > other.lower) { lower = this.lower; lowerInc = this.lowerInc; }
    else if (this.lower < other.lower) { lower = other.lower; lowerInc = other.lowerInc; }
    else { lower = this.lower; lowerInc = this.lowerInc && other.lowerInc; }

    let upper, upperInc;
    if (this.upper === null) { upper = other.upper; upperInc = other.upperInc; }
    else if (other.upper === null) { upper = this.upper; upperInc = this.upperInc; }
    else if (this.upper < other.upper) { upper = this.upper; upperInc = this.upperInc; }
    else if (this.upper > other.upper) { upper = other.upper; upperInc = other.upperInc; }
    else { upper = this.upper; upperInc = this.upperInc && other.upperInc; }

    return new Range(lower, upper, { lowerInc, upperInc });
  }

  /**
   * Union of two ranges (+). Must overlap or be adjacent.
   */
  union(other) {
    if (this.empty) return new Range(other.lower, other.upper, { lowerInc: other.lowerInc, upperInc: other.upperInc });
    if (other.empty) return new Range(this.lower, this.upper, { lowerInc: this.lowerInc, upperInc: this.upperInc });

    let lower, lowerInc;
    if (this.lower === null || other.lower === null) { lower = null; lowerInc = true; }
    else if (this.lower < other.lower) { lower = this.lower; lowerInc = this.lowerInc; }
    else if (this.lower > other.lower) { lower = other.lower; lowerInc = other.lowerInc; }
    else { lower = this.lower; lowerInc = this.lowerInc || other.lowerInc; }

    let upper, upperInc;
    if (this.upper === null || other.upper === null) { upper = null; upperInc = true; }
    else if (this.upper > other.upper) { upper = this.upper; upperInc = this.upperInc; }
    else if (this.upper < other.upper) { upper = other.upper; upperInc = other.upperInc; }
    else { upper = this.upper; upperInc = this.upperInc || other.upperInc; }

    return new Range(lower, upper, { lowerInc, upperInc });
  }

  /**
   * Check if adjacent to another range (-|-).
   */
  isAdjacentTo(other) {
    if (this.empty || other.empty) return false;
    return (
      (this.upper === other.lower && (this.upperInc !== other.lowerInc)) ||
      (other.upper === this.lower && (other.upperInc !== this.lowerInc))
    );
  }

  /**
   * Check if range is empty.
   */
  isEmpty() {
    if (this.empty) return true;
    if (this.lower !== null && this.upper !== null) {
      if (this.lower > this.upper) return true;
      if (this.lower === this.upper && !(this.lowerInc && this.upperInc)) return true;
    }
    return false;
  }

  toString() {
    if (this.empty || this.isEmpty()) return 'empty';
    const lb = this.lowerInc ? '[' : '(';
    const ub = this.upperInc ? ']' : ')';
    const l = this.lower === null ? '' : String(this.lower);
    const u = this.upper === null ? '' : String(this.upper);
    return `${lb}${l},${u}${ub}`;
  }
}

/**
 * RangeTypeManager — manages range type constructors.
 */
export class RangeTypeManager {
  constructor() {
    this._types = new Map();
    // Register built-in range types
    this.register('int4range', Number);
    this.register('int8range', Number);
    this.register('numrange', Number);
    this.register('tsrange', (s) => new Date(s).getTime());
    this.register('daterange', (s) => new Date(s).getTime());
  }

  register(name, parser) {
    this._types.set(name.toLowerCase(), parser);
  }

  create(typeName, lower, upper, options = {}) {
    return new Range(lower, upper, options);
  }

  parse(typeName, str) {
    const parser = this._types.get(typeName.toLowerCase());
    if (!parser) throw new Error(`Range type '${typeName}' does not exist`);
    return Range.fromString(str, parser);
  }

  has(name) { return this._types.has(name.toLowerCase()); }
  list() { return [...this._types.keys()]; }
}
