// partitioning.js — Range partitioning for HenryDB
// Splits a logical table across multiple physical partitions based on key ranges.

/**
 * Range-based table partitioning.
 * Rows are routed to partitions based on a partition key value.
 */
export class RangePartitioner {
  constructor(column, ranges) {
    // ranges: [{ name, minValue, maxValue }]
    this._column = column;
    this._partitions = ranges.map(r => ({
      name: r.name,
      min: r.minValue,
      max: r.maxValue,
      rows: [],
    }));
  }

  /**
   * Route a row to the appropriate partition.
   */
  route(row) {
    const value = row[this._column];
    for (const part of this._partitions) {
      if (value >= part.min && value <= part.max) {
        return part.name;
      }
    }
    return null; // No matching partition
  }

  /**
   * Insert a row into the correct partition.
   */
  insert(row) {
    const partName = this.route(row);
    if (!partName) throw new Error(`No partition found for ${this._column}=${row[this._column]}`);
    const part = this._partitions.find(p => p.name === partName);
    part.rows.push(row);
    return partName;
  }

  /**
   * Query with partition pruning.
   * Only scans partitions that could contain matching rows.
   */
  query(predicate, minValue = null, maxValue = null) {
    let relevantPartitions = this._partitions;
    
    // Partition pruning: only scan partitions that overlap the query range
    if (minValue !== null || maxValue !== null) {
      relevantPartitions = this._partitions.filter(p => {
        if (minValue !== null && p.max < minValue) return false;
        if (maxValue !== null && p.min > maxValue) return false;
        return true;
      });
    }
    
    const results = [];
    for (const part of relevantPartitions) {
      for (const row of part.rows) {
        if (!predicate || predicate(row)) results.push(row);
      }
    }
    return results;
  }

  /**
   * Get partition statistics.
   */
  stats() {
    return this._partitions.map(p => ({
      name: p.name,
      min: p.min,
      max: p.max,
      rowCount: p.rows.length,
    }));
  }

  /**
   * Get number of partitions that would be scanned for a range query.
   */
  prunedPartitionCount(minValue, maxValue) {
    return this._partitions.filter(p => {
      if (p.max < minValue) return false;
      if (p.min > maxValue) return false;
      return true;
    }).length;
  }
}
