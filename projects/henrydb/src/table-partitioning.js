// table-partitioning.js — Range and hash partitioning
// Divides a table into partitions for parallelism and pruning.

export class PartitionedTable {
  constructor(name, partitionKey, strategy, options = {}) {
    this.name = name;
    this.partitionKey = partitionKey;
    this.strategy = strategy; // 'range' | 'hash'
    this._partitions = new Map(); // partitionId → rows[]
    
    if (strategy === 'range') {
      this.boundaries = options.boundaries || []; // [100, 200, 300] creates 4 partitions
    } else {
      this.numPartitions = options.numPartitions || 4;
      for (let i = 0; i < this.numPartitions; i++) this._partitions.set(i, []);
    }
  }

  insert(row) {
    const pid = this._getPartition(row[this.partitionKey]);
    if (!this._partitions.has(pid)) this._partitions.set(pid, []);
    this._partitions.get(pid).push(row);
  }

  /**
   * Scan with partition pruning. Only scans relevant partitions.
   */
  scan(predicate) {
    const partitions = predicate ? this._prunePartitions(predicate) : [...this._partitions.keys()];
    const results = [];
    for (const pid of partitions) {
      const rows = this._partitions.get(pid) || [];
      for (const row of rows) {
        if (!predicate || predicate(row)) results.push(row);
      }
    }
    return results;
  }

  scanAll() {
    const results = [];
    for (const rows of this._partitions.values()) results.push(...rows);
    return results;
  }

  _getPartition(value) {
    if (this.strategy === 'hash') {
      let h = 0;
      const s = String(value);
      for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
      return ((h >>> 0) % this.numPartitions);
    }
    // Range
    for (let i = 0; i < this.boundaries.length; i++) {
      if (value < this.boundaries[i]) return i;
    }
    return this.boundaries.length;
  }

  _prunePartitions(predicate) {
    // For range partitioning, we can prune based on boundaries
    // For now, scan all partitions (conservative)
    return [...this._partitions.keys()];
  }

  /**
   * Range partition pruning: only scan partitions that could contain values in [lo, hi].
   */
  rangeScan(lo, hi) {
    if (this.strategy !== 'range') return this.scanAll().filter(r => r[this.partitionKey] >= lo && r[this.partitionKey] <= hi);

    const results = [];
    for (const [pid, rows] of this._partitions) {
      // Determine partition range
      const pLo = pid === 0 ? -Infinity : this.boundaries[pid - 1];
      const pHi = pid < this.boundaries.length ? this.boundaries[pid] : Infinity;
      
      // Skip if partition doesn't overlap [lo, hi]
      if (pHi <= lo || pLo > hi) continue;
      
      for (const row of rows) {
        const v = row[this.partitionKey];
        if (v >= lo && v <= hi) results.push(row);
      }
    }
    return results;
  }

  get partitionCount() { return this._partitions.size; }
  get totalRows() { let n = 0; for (const p of this._partitions.values()) n += p.length; return n; }

  getPartitionSizes() {
    const sizes = {};
    for (const [pid, rows] of this._partitions) sizes[pid] = rows.length;
    return sizes;
  }
}
