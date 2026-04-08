// bloom-join.js — Bloom filter semi-join reducer
// Before doing an expensive hash join, build a Bloom filter on one side
// and use it to filter the other side, eliminating non-matching rows early.
// This is particularly effective for multi-way joins where intermediate results
// can be pruned before they explode in size.
//
// Based on: Bloom filter semi-joins in distributed query processing.

import { BloomFilter } from './bloom-filter.js';

/**
 * BloomJoin — Bloom filter-based semi-join reducer.
 */
export class BloomJoin {
  constructor(options = {}) {
    this.falsePositiveRate = options.falsePositiveRate || 0.01; // 1% FP rate
    this.stats = { buildTimeMs: 0, probeTimeMs: 0, inputRows: 0, filteredRows: 0, passedRows: 0, fpRate: 0 };
  }

  /**
   * Build a Bloom filter from the join key column of one side.
   * Returns the filter for use in probing.
   */
  buildFilter(keyColumn) {
    const t0 = Date.now();
    const uniqueKeys = new Set(keyColumn);
    const bf = new BloomFilter(uniqueKeys.size, this.falsePositiveRate);
    
    for (const key of uniqueKeys) {
      bf.add(String(key));
    }
    
    this.stats.buildTimeMs = Date.now() - t0;
    return bf;
  }

  /**
   * Use a Bloom filter to pre-filter the other side of a join.
   * Returns indices of rows that might match (including false positives).
   */
  probeFilter(keyColumn, bloomFilter) {
    const t0 = Date.now();
    const result = [];
    this.stats.inputRows = keyColumn.length;

    for (let i = 0; i < keyColumn.length; i++) {
      if (bloomFilter.mightContain(String(keyColumn[i]))) {
        result.push(i);
      }
    }

    this.stats.probeTimeMs = Date.now() - t0;
    this.stats.passedRows = result.length;
    this.stats.filteredRows = keyColumn.length - result.length;
    
    return new Uint32Array(result);
  }

  /**
   * Bloom-filtered hash join: build Bloom filter → pre-filter → hash join.
   * The full pipeline for a two-way join.
   */
  join(leftKeyColumn, rightKeyColumn, leftData, rightData) {
    const totalStart = Date.now();

    // Build Bloom filter on the (usually smaller) right side
    const bf = this.buildFilter(rightKeyColumn);

    // Pre-filter left side using Bloom filter
    const candidateLeftIndices = this.probeFilter(leftKeyColumn, bf);

    // Now do a regular hash join, but only on candidates
    const hashTable = new Map();
    for (let i = 0; i < rightKeyColumn.length; i++) {
      const key = rightKeyColumn[i];
      if (!hashTable.has(key)) hashTable.set(key, []);
      hashTable.get(key).push(i);
    }

    const leftResults = [];
    const rightResults = [];
    
    for (const lIdx of candidateLeftIndices) {
      const key = leftKeyColumn[lIdx];
      const matches = hashTable.get(key);
      if (matches) {
        for (const rIdx of matches) {
          leftResults.push(lIdx);
          rightResults.push(rIdx);
        }
      }
    }

    this.stats.totalMs = Date.now() - totalStart;
    
    // Calculate actual false positive rate
    const truePositives = leftResults.length;
    const bloomPassed = candidateLeftIndices.length;
    const falsePositives = bloomPassed - truePositives;
    this.stats.fpRate = bloomPassed > 0 ? (falsePositives / bloomPassed * 100).toFixed(2) + '%' : '0%';

    return {
      left: new Uint32Array(leftResults),
      right: new Uint32Array(rightResults),
      stats: this.getStats(),
    };
  }

  /**
   * Multi-way Bloom filter reduction.
   * For a chain of joins A ⋈ B ⋈ C, build Bloom filters from B and C
   * to pre-filter A before any hash join.
   */
  multiWayReduce(mainKeyColumn, sideKeyColumns) {
    const filters = sideKeyColumns.map(col => this.buildFilter(col));
    
    // Intersect all Bloom filters: a row passes only if ALL filters say "maybe"
    const result = [];
    for (let i = 0; i < mainKeyColumn.length; i++) {
      const keyStr = String(mainKeyColumn[i]);
      if (filters.every(bf => bf.mightContain(keyStr))) {
        result.push(i);
      }
    }

    return new Uint32Array(result);
  }

  getStats() {
    return { ...this.stats };
  }
}
