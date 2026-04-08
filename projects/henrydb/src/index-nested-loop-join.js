// index-nested-loop-join.js — Index nested loop join
// For each row in the outer table, use an index to find matching rows in the inner table.
// Optimal when: outer is small, inner has an index on the join column.
// Time complexity: O(|outer| * log(|inner|)) vs hash join's O(|outer| + |inner|).

/**
 * IndexNestedLoopJoin — uses a Map-based index for O(1) lookups.
 */
export class IndexNestedLoopJoin {
  constructor() {
    this.stats = { outerRows: 0, indexLookups: 0, matches: 0, timeMs: 0 };
  }

  /**
   * Join using an index on the inner (right) table.
   * @param {any[]} outerKeys — outer table join key column
   * @param {Map} innerIndex — pre-built index: key → [row indices]
   */
  join(outerKeys, innerIndex) {
    const t0 = Date.now();
    this.stats.outerRows = outerKeys.length;

    const leftIndices = [];
    const rightIndices = [];

    for (let i = 0; i < outerKeys.length; i++) {
      this.stats.indexLookups++;
      const matches = innerIndex.get(outerKeys[i]);
      if (matches) {
        for (const rIdx of matches) {
          leftIndices.push(i);
          rightIndices.push(rIdx);
        }
      }
    }

    this.stats.matches = leftIndices.length;
    this.stats.timeMs = Date.now() - t0;

    return {
      left: new Uint32Array(leftIndices),
      right: new Uint32Array(rightIndices),
    };
  }

  /**
   * Build an index from a key column.
   */
  static buildIndex(keys) {
    const index = new Map();
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      if (!index.has(key)) index.set(key, []);
      index.get(key).push(i);
    }
    return index;
  }

  /**
   * Semi-join: for each outer row, check if ANY match exists (EXISTS).
   * Returns outer indices that have at least one match.
   */
  semiJoin(outerKeys, innerIndex) {
    const t0 = Date.now();
    const result = [];

    for (let i = 0; i < outerKeys.length; i++) {
      if (innerIndex.has(outerKeys[i])) {
        result.push(i);
      }
    }

    this.stats.timeMs = Date.now() - t0;
    this.stats.outerRows = outerKeys.length;
    this.stats.matches = result.length;
    return new Uint32Array(result);
  }

  /**
   * Anti-join: for each outer row, check if NO match exists (NOT EXISTS).
   * Returns outer indices that have NO match.
   */
  antiJoin(outerKeys, innerIndex) {
    const t0 = Date.now();
    const result = [];

    for (let i = 0; i < outerKeys.length; i++) {
      if (!innerIndex.has(outerKeys[i])) {
        result.push(i);
      }
    }

    this.stats.timeMs = Date.now() - t0;
    this.stats.outerRows = outerKeys.length;
    this.stats.matches = result.length;
    return new Uint32Array(result);
  }

  getStats() { return { ...this.stats }; }
}
