// symmetric-hash-join.js — Symmetric hash join for pipelined execution
// Standard hash join: build hash table on one side, then probe with other.
// Symmetric hash join: build hash tables on BOTH sides simultaneously.
// As each row arrives from either input, probe the other side's hash table
// and add it to its own. This enables streaming/pipelined execution.
//
// Used in: streaming systems, adaptive query processing, pipeline parallelism.

/**
 * SymmetricHashJoin — processes both inputs incrementally.
 */
export class SymmetricHashJoin {
  constructor(leftKeyFn, rightKeyFn) {
    this.leftKeyFn = leftKeyFn || (r => r);
    this.rightKeyFn = rightKeyFn || (r => r);
    this._leftHT = new Map(); // key → [row indices]
    this._rightHT = new Map();
    this._leftRows = [];
    this._rightRows = [];
    this._results = [];
    this.stats = { leftProcessed: 0, rightProcessed: 0, matches: 0 };
  }

  /**
   * Process a row from the left input.
   * Returns any new matches produced.
   */
  processLeft(row) {
    const key = this.leftKeyFn(row);
    const idx = this._leftRows.length;
    this._leftRows.push(row);

    // Add to left hash table
    if (!this._leftHT.has(key)) this._leftHT.set(key, []);
    this._leftHT.get(key).push(idx);

    // Probe right hash table for matches
    const matches = [];
    const rightMatches = this._rightHT.get(key);
    if (rightMatches) {
      for (const rIdx of rightMatches) {
        const pair = { left: idx, right: rIdx };
        this._results.push(pair);
        matches.push(pair);
      }
    }

    this.stats.leftProcessed++;
    this.stats.matches += matches.length;
    return matches;
  }

  /**
   * Process a row from the right input.
   * Returns any new matches produced.
   */
  processRight(row) {
    const key = this.rightKeyFn(row);
    const idx = this._rightRows.length;
    this._rightRows.push(row);

    // Add to right hash table
    if (!this._rightHT.has(key)) this._rightHT.set(key, []);
    this._rightHT.get(key).push(idx);

    // Probe left hash table for matches
    const matches = [];
    const leftMatches = this._leftHT.get(key);
    if (leftMatches) {
      for (const lIdx of leftMatches) {
        const pair = { left: lIdx, right: idx };
        this._results.push(pair);
        matches.push(pair);
      }
    }

    this.stats.rightProcessed++;
    this.stats.matches += matches.length;
    return matches;
  }

  /**
   * Batch process: interleave left and right rows.
   * Simulates a streaming scenario where rows arrive from both sides.
   */
  processBatch(leftRows, rightRows) {
    const allMatches = [];
    const li = leftRows.length;
    const ri = rightRows.length;
    let l = 0, r = 0;

    // Interleave: process one from each side alternately
    while (l < li || r < ri) {
      if (l < li) {
        const matches = this.processLeft(leftRows[l++]);
        allMatches.push(...matches);
      }
      if (r < ri) {
        const matches = this.processRight(rightRows[r++]);
        allMatches.push(...matches);
      }
    }

    return allMatches;
  }

  /**
   * Get all accumulated results.
   */
  getResults() {
    return this._results;
  }

  /**
   * Materialize results as row objects.
   */
  materialize(leftCols, rightCols) {
    const rows = [];
    for (const { left, right } of this._results) {
      const row = {};
      const lRow = this._leftRows[left];
      const rRow = this._rightRows[right];

      if (leftCols) {
        for (const col of leftCols) row[col] = lRow[col];
      } else {
        Object.assign(row, lRow);
      }

      if (rightCols) {
        for (const col of rightCols) {
          const key = col in row ? `right.${col}` : col;
          row[key] = rRow[col];
        }
      } else {
        for (const [k, v] of Object.entries(rRow)) {
          const key = k in row ? `right.${k}` : k;
          row[key] = v;
        }
      }

      rows.push(row);
    }
    return rows;
  }

  get totalMatches() { return this._results.length; }
  getStats() { return { ...this.stats }; }
}
