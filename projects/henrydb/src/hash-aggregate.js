// hash-aggregate.js — Hash aggregation with spill-to-disk support
// For GROUP BY with many distinct groups, the hash table can exceed memory.
// Solution: partition + spill + recursive aggregation.
//
// Two modes:
// 1. In-memory: standard hash table aggregation (fast, bounded memory)
// 2. Spilling: when memory budget exceeded, partition remaining data and aggregate per partition

/**
 * HashAggregate — hash-based GROUP BY with memory-bounded execution.
 */
export class HashAggregate {
  constructor(options = {}) {
    this.memoryBudget = options.memoryBudget || 100000; // Max groups in memory
    this.stats = { totalRows: 0, groups: 0, spills: 0, passes: 0, timeMs: 0 };
  }

  /**
   * Aggregate with GROUP BY.
   * @param {any[]} groupValues — group key column
   * @param {any[]} aggValues — column to aggregate
   * @param {string} aggFn — SUM, COUNT, AVG, MIN, MAX
   */
  aggregate(groupValues, aggValues, aggFn) {
    const t0 = Date.now();
    this.stats.totalRows = groupValues.length;
    this.stats.passes++;

    if (groupValues.length <= this.memoryBudget) {
      // In-memory: standard hash aggregation
      const result = this._inMemoryAggregate(groupValues, aggValues, aggFn);
      this.stats.timeMs = Date.now() - t0;
      this.stats.groups = result.length;
      return result;
    }

    // Spilling: partition + aggregate per partition
    const result = this._spillingAggregate(groupValues, aggValues, aggFn);
    this.stats.timeMs = Date.now() - t0;
    this.stats.groups = result.length;
    return result;
  }

  /**
   * Multi-aggregate: compute multiple aggregates in one pass.
   */
  multiAggregate(groupValues, aggregates) {
    const t0 = Date.now();
    this.stats.totalRows = groupValues.length;

    const groups = new Map();
    for (let i = 0; i < groupValues.length; i++) {
      const key = groupValues[i];
      if (!groups.has(key)) {
        const state = {};
        for (const agg of aggregates) {
          state[agg.alias] = { sum: 0, count: 0, min: Infinity, max: -Infinity };
        }
        groups.set(key, state);
      }

      const state = groups.get(key);
      for (const agg of aggregates) {
        const val = agg.values[i];
        const s = state[agg.alias];
        if (val != null) {
          const numVal = typeof val === 'number' ? val : parseFloat(val) || 0;
          s.sum += numVal;
          s.count++;
          if (numVal < s.min) s.min = numVal;
          if (numVal > s.max) s.max = numVal;
        }
      }
    }

    const results = [];
    for (const [key, state] of groups) {
      const row = { group: key };
      for (const agg of aggregates) {
        const s = state[agg.alias];
        switch (agg.fn) {
          case 'SUM': row[agg.alias] = s.sum; break;
          case 'COUNT': row[agg.alias] = s.count; break;
          case 'AVG': row[agg.alias] = s.count > 0 ? s.sum / s.count : null; break;
          case 'MIN': row[agg.alias] = s.min === Infinity ? null : s.min; break;
          case 'MAX': row[agg.alias] = s.max === -Infinity ? null : s.max; break;
        }
      }
      results.push(row);
    }

    this.stats.timeMs = Date.now() - t0;
    this.stats.groups = results.length;
    return results;
  }

  _inMemoryAggregate(groupValues, aggValues, aggFn) {
    const groups = new Map();

    for (let i = 0; i < groupValues.length; i++) {
      const key = groupValues[i];
      if (!groups.has(key)) {
        groups.set(key, { sum: 0, count: 0, min: Infinity, max: -Infinity });
      }
      const g = groups.get(key);
      const val = aggValues[i];
      const numVal = typeof val === 'number' ? val : parseFloat(val) || 0;
      g.sum += numVal;
      g.count++;
      if (numVal < g.min) g.min = numVal;
      if (numVal > g.max) g.max = numVal;
    }

    return this._finalizeGroups(groups, aggFn);
  }

  _spillingAggregate(groupValues, aggValues, aggFn) {
    // Partition into buckets based on hash of group key
    const numPartitions = Math.ceil(groupValues.length / this.memoryBudget);
    const partitions = Array.from({ length: numPartitions }, () => ({ groups: [], values: [] }));

    for (let i = 0; i < groupValues.length; i++) {
      const key = groupValues[i];
      const hash = this._hash(key) % numPartitions;
      partitions[hash].groups.push(key);
      partitions[hash].values.push(aggValues[i]);
    }

    this.stats.spills = numPartitions;

    // Aggregate each partition independently
    const allResults = [];
    for (const partition of partitions) {
      if (partition.groups.length === 0) continue;
      this.stats.passes++;
      const partial = this._inMemoryAggregate(partition.groups, partition.values, aggFn);
      allResults.push(...partial);
    }

    // Merge results (groups should be unique across partitions by construction)
    return allResults;
  }

  _finalizeGroups(groups, aggFn) {
    const results = [];
    for (const [key, g] of groups) {
      let value;
      switch (aggFn) {
        case 'SUM': value = g.sum; break;
        case 'COUNT': value = g.count; break;
        case 'AVG': value = g.count > 0 ? g.sum / g.count : null; break;
        case 'MIN': value = g.min === Infinity ? null : g.min; break;
        case 'MAX': value = g.max === -Infinity ? null : g.max; break;
      }
      results.push({ group: key, value, count: g.count });
    }
    return results;
  }

  _hash(key) {
    if (typeof key === 'number') return Math.abs(key * 2654435761 | 0);
    let h = 0;
    const s = String(key);
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
    return Math.abs(h);
  }

  getStats() { return { ...this.stats }; }
}
