// partitioning.js — Table partitioning engine for HenryDB
// Supports RANGE, LIST, and HASH partitioning strategies.
// Includes partition pruning for query optimization.

/**
 * PartitionStrategy — base class for partitioning strategies.
 */
class PartitionStrategy {
  constructor(column) {
    this.column = column;
  }

  /**
   * Determine which partition(s) a row belongs to.
   * Returns partition name(s).
   */
  route(row) { throw new Error('Not implemented'); }

  /**
   * Prune partitions based on a WHERE condition.
   * Returns the set of partition names that could contain matching rows.
   */
  prune(condition) { throw new Error('Not implemented'); }
}

/**
 * RangePartition — partition by value ranges.
 * 
 * Example: PARTITION BY RANGE (date)
 *   p2024q1 FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')
 *   p2024q2 FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')
 */
class RangePartition extends PartitionStrategy {
  constructor(column, ranges) {
    super(column);
    this.ranges = ranges; // [{ name, from, to }] (sorted by from)
    this.ranges.sort((a, b) => (a.from < b.from ? -1 : a.from > b.from ? 1 : 0));
  }

  route(row) {
    const value = row[this.column];
    if (value === undefined || value === null) return null;

    for (const range of this.ranges) {
      if (value >= range.from && value < range.to) {
        return range.name;
      }
    }
    return null; // No matching partition (constraint violation)
  }

  prune(condition) {
    if (!condition) return this.ranges.map(r => r.name); // No pruning possible

    const { op, column, value } = condition;
    if (column !== this.column) return this.ranges.map(r => r.name);

    const matching = [];
    for (const range of this.ranges) {
      switch (op) {
        case '=':
          if (value >= range.from && value < range.to) matching.push(range.name);
          break;
        case '>':
          if (range.to > value) matching.push(range.name);
          break;
        case '>=':
          if (range.to > value || (range.from <= value && value < range.to)) matching.push(range.name);
          break;
        case '<':
          if (range.from < value) matching.push(range.name);
          break;
        case '<=':
          if (range.from <= value) matching.push(range.name);
          break;
        case 'BETWEEN':
          if (range.to > value[0] && range.from < value[1]) matching.push(range.name);
          break;
        default:
          matching.push(range.name);
      }
    }
    return matching.length > 0 ? matching : this.ranges.map(r => r.name);
  }
}

/**
 * ListPartition — partition by exact value lists.
 * 
 * Example: PARTITION BY LIST (region)
 *   p_east FOR VALUES IN ('NY', 'NJ', 'CT')
 *   p_west FOR VALUES IN ('CA', 'OR', 'WA')
 */
class ListPartition extends PartitionStrategy {
  constructor(column, lists) {
    super(column);
    this.lists = lists; // [{ name, values: Set }]
    this._valueToPartition = new Map();
    for (const list of lists) {
      for (const val of list.values) {
        this._valueToPartition.set(val, list.name);
      }
    }
  }

  route(row) {
    const value = row[this.column];
    return this._valueToPartition.get(value) || null;
  }

  prune(condition) {
    if (!condition) return this.lists.map(l => l.name);

    const { op, column, value } = condition;
    if (column !== this.column) return this.lists.map(l => l.name);

    if (op === '=') {
      const partName = this._valueToPartition.get(value);
      return partName ? [partName] : [];
    }

    if (op === 'IN' && Array.isArray(value)) {
      const partitions = new Set();
      for (const v of value) {
        const p = this._valueToPartition.get(v);
        if (p) partitions.add(p);
      }
      return [...partitions];
    }

    return this.lists.map(l => l.name);
  }
}

/**
 * HashPartition — partition by hash of the partition key.
 * 
 * Example: PARTITION BY HASH (user_id) PARTITIONS 4
 */
class HashPartition extends PartitionStrategy {
  constructor(column, numPartitions) {
    super(column);
    this.numPartitions = numPartitions;
    this.partitionNames = [];
    for (let i = 0; i < numPartitions; i++) {
      this.partitionNames.push(`p${i}`);
    }
  }

  route(row) {
    const value = row[this.column];
    if (value === undefined || value === null) return this.partitionNames[0];
    const hash = this._hash(value);
    const idx = Math.abs(hash) % this.numPartitions;
    return this.partitionNames[idx];
  }

  prune(condition) {
    if (!condition) return [...this.partitionNames];

    const { op, column, value } = condition;
    if (column !== this.column) return [...this.partitionNames];

    // Hash partitioning can only prune on equality
    if (op === '=') {
      const hash = this._hash(value);
      const idx = Math.abs(hash) % this.numPartitions;
      return [this.partitionNames[idx]];
    }

    return [...this.partitionNames]; // Can't prune range conditions on hash
  }

  _hash(value) {
    let hash = 0;
    const str = String(value);
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) - hash) + str.charCodeAt(i);
      hash |= 0;
    }
    return hash;
  }
}

/**
 * PartitionedTable — a table split into partitions.
 */
export class PartitionedTable {
  constructor(name, columns, strategy) {
    this.name = name;
    this.columns = columns;
    this.strategy = strategy;
    this.partitions = new Map(); // partition name → row array
    this._totalRows = 0;

    // Initialize partitions
    const allNames = strategy instanceof RangePartition ? strategy.ranges.map(r => r.name)
      : strategy instanceof ListPartition ? strategy.lists.map(l => l.name)
      : strategy.partitionNames;
    for (const name of allNames) {
      this.partitions.set(name, []);
    }
  }

  /**
   * Insert a row, routing to the correct partition.
   */
  insert(row) {
    const partName = this.strategy.route(row);
    if (!partName) {
      throw new Error(`No matching partition for row: ${JSON.stringify(row)}`);
    }
    const partition = this.partitions.get(partName);
    if (!partition) {
      throw new Error(`Partition '${partName}' does not exist`);
    }
    partition.push(row);
    this._totalRows++;
    return partName;
  }

  /**
   * Query with optional partition pruning.
   * @param {object|null} condition - { op, column, value }
   * @returns {{ rows: object[], partitionsScanned: string[], partitionsPruned: number }}
   */
  query(condition = null) {
    const allPartitions = [...this.partitions.keys()];
    const targetPartitions = condition
      ? this.strategy.prune(condition)
      : allPartitions;

    const rows = [];
    for (const pName of targetPartitions) {
      const partition = this.partitions.get(pName);
      if (!partition) continue;

      for (const row of partition) {
        if (!condition || this._matchCondition(row, condition)) {
          rows.push(row);
        }
      }
    }

    return {
      rows,
      partitionsScanned: targetPartitions,
      partitionsPruned: allPartitions.length - targetPartitions.length,
    };
  }

  /**
   * Delete rows matching a condition.
   */
  delete(condition) {
    const targetPartitions = condition
      ? this.strategy.prune(condition)
      : [...this.partitions.keys()];

    let deleted = 0;
    for (const pName of targetPartitions) {
      const partition = this.partitions.get(pName);
      if (!partition) continue;

      const before = partition.length;
      const kept = partition.filter(row => !this._matchCondition(row, condition));
      this.partitions.set(pName, kept);
      deleted += before - kept.length;
    }
    this._totalRows -= deleted;
    return deleted;
  }

  getStats() {
    const partitionStats = {};
    for (const [name, rows] of this.partitions) {
      partitionStats[name] = rows.length;
    }
    return {
      name: this.name,
      totalRows: this._totalRows,
      partitionCount: this.partitions.size,
      strategy: this.strategy.constructor.name,
      column: this.strategy.column,
      partitions: partitionStats,
    };
  }

  _matchCondition(row, condition) {
    const value = row[condition.column];
    switch (condition.op) {
      case '=': return value == condition.value;
      case '!=': return value != condition.value;
      case '>': return value > condition.value;
      case '<': return value < condition.value;
      case '>=': return value >= condition.value;
      case '<=': return value <= condition.value;
      case 'IN': return condition.value.includes(value);
      case 'BETWEEN': return value >= condition.value[0] && value <= condition.value[1];
      default: return true;
    }
  }
}

// Factory functions
export function createRangePartition(column, ranges) {
  return new RangePartition(column, ranges);
}

export function createListPartition(column, lists) {
  return new ListPartition(column, lists.map(l => ({ name: l.name, values: new Set(l.values) })));
}

export function createHashPartition(column, numPartitions) {
  return new HashPartition(column, numPartitions);
}
