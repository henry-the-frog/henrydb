// theta-join.js — Nested loop join with compiled arbitrary predicates
// A theta join evaluates an arbitrary predicate (not just equality) for every
// pair of rows from the two inputs. The predicate is compiled into a function
// for maximum throughput.

/**
 * ThetaJoin — nested loop with compiled predicate.
 */
export class ThetaJoin {
  constructor() {
    this.stats = { leftRows: 0, rightRows: 0, comparisons: 0, matches: 0, timeMs: 0 };
  }

  /**
   * Join with an arbitrary predicate.
   * @param {any[]} leftRows — left table rows
   * @param {any[]} rightRows — right table rows
   * @param {Function} predicate — (leftRow, rightRow) => boolean
   * @param {number} limit — max results
   */
  join(leftRows, rightRows, predicate, limit = Infinity) {
    const t0 = Date.now();
    this.stats.leftRows = leftRows.length;
    this.stats.rightRows = rightRows.length;

    const leftIndices = [];
    const rightIndices = [];

    for (let l = 0; l < leftRows.length && leftIndices.length < limit; l++) {
      for (let r = 0; r < rightRows.length && leftIndices.length < limit; r++) {
        this.stats.comparisons++;
        if (predicate(leftRows[l], rightRows[r])) {
          leftIndices.push(l);
          rightIndices.push(r);
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
   * Block nested loop: process blocks of left rows for better cache locality.
   */
  blockJoin(leftRows, rightRows, predicate, blockSize = 64, limit = Infinity) {
    const t0 = Date.now();
    this.stats.leftRows = leftRows.length;
    this.stats.rightRows = rightRows.length;

    const leftIndices = [];
    const rightIndices = [];

    for (let lBlock = 0; lBlock < leftRows.length && leftIndices.length < limit; lBlock += blockSize) {
      const lEnd = Math.min(lBlock + blockSize, leftRows.length);

      for (let r = 0; r < rightRows.length && leftIndices.length < limit; r++) {
        for (let l = lBlock; l < lEnd && leftIndices.length < limit; l++) {
          this.stats.comparisons++;
          if (predicate(leftRows[l], rightRows[r])) {
            leftIndices.push(l);
            rightIndices.push(r);
          }
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

  getStats() { return { ...this.stats }; }
}
