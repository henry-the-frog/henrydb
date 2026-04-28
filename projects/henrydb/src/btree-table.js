// btree-table.js — Clustered B+tree table storage for HenryDB
// Unlike HeapFile (unordered), BTreeTable stores rows sorted by primary key.
// This enables O(log n) point lookups and ordered range scans without a separate index.
//
// Architecture:
//   - Uses BPlusTree internally with PK as key, full row as value
//   - Provides the same interface as HeapFile for db.js compatibility
//   - "pageId" and "slotIdx" in the API are synthetic (based on insertion order)
//     but point lookups use the B+tree directly via PK
//
// Trade-offs vs HeapFile:
//   - Faster: point lookups (O(log n) vs O(n)), range scans (ordered), ORDER BY on PK
//   - Slower: random inserts (may cause page splits), deletes (may cause merges)
//   - More memory: internal nodes overhead

import { BPlusTree } from './bplus-tree.js';

/**
 * Extract a comparable key from a row's primary key column(s).
 * For composite keys, creates a string representation.
 */
function makeKey(values, pkIndices) {
  if (pkIndices.length === 1) {
    return values[pkIndices[0]];
  }
  return pkIndices.map(i => String(values[i])).join('\0');
}

/**
 * BTreeTable — Clustered B+tree table storage engine.
 * Rows are stored in B+tree leaf nodes, sorted by primary key.
 * Provides HeapFile-compatible interface for db.js.
 */
export class BTreeTable {
  /**
   * @param {string} name - Table name
   * @param {Object} options
   * @param {number[]} options.pkIndices - Column indices that form the primary key (default: [0])
   * @param {number} options.order - B+tree order/branching factor (default: 64)
   */
  constructor(name, options = {}) {
    this.name = name;
    this.pkIndices = options.pkIndices || [0];
    this.order = options.order || 64;
    this._tree = new BPlusTree(this.order);
    this._rowCount = 0;
    
    // For HeapFile compatibility: maintain synthetic rid <-> pk mappings
    this._nextRid = 0;
    this._ridToPk = new Map();   // rid (number) -> pk
    this._pkToRid = new Map();   // pk -> {pageId, slotIdx}
    
    // Dead tuples: old versions kept for MVCC snapshot reads.
    // Maps "pageId:slotIdx" -> values (the old row data before UPDATE replaced it).
    this._deadTuples = new Map();
    
    // HOT chain: maps "oldPageId:oldSlotIdx" → {pageId, slotIdx} of the new version.
    this._hotChains = new Map();
    
    this._syntheticPageSize = 100;
    this._pages = []; // not used but HeapFile compat
  }

  get rowCount() { return this._rowCount; }
  get tupleCount() { return this._rowCount; }
  get pageCount() { return Math.max(1, Math.ceil(this._rowCount / this._syntheticPageSize)); }

  /**
   * Insert a row. Returns a HeapFile-compatible rid: {pageId, slotIdx}.
   */
  insert(values) {
    const pk = makeKey(values, this.pkIndices);
    
    // Check for duplicate PK
    const existing = this._tree.get(pk);
    const oldRid = this._pkToRid.get(pk);
    
    if (existing !== undefined && oldRid) {
      // PK already exists (UPDATE scenario). Save old values as dead tuple
      // so MVCC snapshot reads can still find the old version.
      const oldKey = `${oldRid.pageId}:${oldRid.slotIdx}`;
      this._deadTuples.set(oldKey, [...existing]); // Clone the old values
    }
    
    // Always create a new rid — MVCC needs separate physical slots for
    // old and new versions even when the PK is the same.
    const ridNum = this._nextRid++;
    const pageId = Math.floor(ridNum / this._syntheticPageSize);
    const slotIdx = ridNum % this._syntheticPageSize;
    
    this._tree.insert(pk, values);
    this._ridToPk.set(ridNum, pk);
    this._pkToRid.set(pk, { pageId, slotIdx });
    
    if (existing === undefined) {
      this._rowCount++;
    }
    
    return { pageId, slotIdx };
  }

  /**
   * Get a row by rid (HeapFile compat).
   */
  get(pageId, slotIdx) {
    // Check live B+tree entries first
    const ridNum = pageId * this._syntheticPageSize + slotIdx;
    const pk = this._ridToPk.get(ridNum);
    if (pk !== undefined) {
      const live = this._tree.get(pk);
      if (live !== undefined) return live;
    }
    
    // Fall back to dead tuples (old versions kept for MVCC)
    const deadKey = `${pageId}:${slotIdx}`;
    const dead = this._deadTuples.get(deadKey);
    if (dead) return dead;
    
    return null;
  }

  /**
   * Look up a row by primary key value. O(log n).
   */
  findByPK(pkValue) {
    return this._tree.get(pkValue) ?? null;
  }

  /**
   * In-place update by rid. Reuses the same RID (no dead tuple, no MVCC).
   * For use in fast-path UPDATE when no concurrent readers need the old version.
   */
  update(pageId, slotIdx, newValues) {
    const ridNum = pageId * this._syntheticPageSize + slotIdx;
    const pk = this._ridToPk.get(ridNum);
    if (pk === undefined) return null;
    
    // Update the B-tree entry in place
    this._tree.insert(pk, newValues);
    return { pageId, slotIdx };
  }

  /**
   * Delete by rid (HeapFile compat).
   */
  delete(pageId, slotIdx) {
    // Check if this is a dead tuple
    const deadKey = `${pageId}:${slotIdx}`;
    if (this._deadTuples.has(deadKey)) {
      this._deadTuples.delete(deadKey);
      return true;
    }
    
    const ridNum = pageId * this._syntheticPageSize + slotIdx;
    const pk = this._ridToPk.get(ridNum);
    if (pk === undefined) return false;
    
    this._tree.delete(pk);
    this._ridToPk.delete(ridNum);
    this._pkToRid.delete(pk);
    this._rowCount--;
    return true;
  }

  /**
   * Delete by primary key. O(log n).
   */
  deleteByPK(pkValue) {
    const ridInfo = this._pkToRid.get(pkValue);
    if (!ridInfo) return false;
    
    this._tree.delete(pkValue);
    const ridNum = ridInfo.pageId * this._syntheticPageSize + ridInfo.slotIdx;
    this._ridToPk.delete(ridNum);
    this._pkToRid.delete(pkValue);
    this._rowCount--;
    return true;
  }

  /**
   * Scan all rows IN PRIMARY KEY ORDER.
   * This is the key advantage over HeapFile — ordered iteration.
   */
  *scan() {
    // Yield live B+tree entries
    for (const { key, value } of this._tree) {
      const rid = this._pkToRid.get(key);
      if (rid) {
        yield { pageId: rid.pageId, slotIdx: rid.slotIdx, values: value };
      }
    }
    // Also yield dead tuples (old versions kept for MVCC snapshot reads)
    for (const [key, values] of this._deadTuples) {
      const [pageId, slotIdx] = key.split(':').map(Number);
      yield { pageId, slotIdx, values };
    }
  }

  /**
   * Range scan: rows where PK is between low and high (inclusive).
   * O(log n + k) where k is number of results.
   */
  *rangeScan(low, high) {
    const results = this._tree.range(low, high);
    for (const { key, value } of results) {
      const rid = this._pkToRid.get(key);
      if (rid) {
        yield { pageId: rid.pageId, slotIdx: rid.slotIdx, values: value };
      }
    }
  }

  /**
   * Point lookup by PK — O(log n). Returns {pageId, slotIdx, values} or null.
   */
  lookupByPK(pkValue) {
    const values = this._tree.get(pkValue);
    if (values === undefined) return null;
    const rid = this._pkToRid.get(pkValue);
    return rid ? { pageId: rid.pageId, slotIdx: rid.slotIdx, values } : null;
  }

  /**
   * Get the minimum PK value.
   */
  minKey() {
    return this._tree.min();
  }

  /**
   * Get the maximum PK value.
   */
  maxKey() {
    return this._tree.max();
  }

  /**
   * Get statistics about the table.
   */
  getStats() {
    return {
      engine: 'btree',
      rows: this._rowCount,
      order: this.order,
      pkIndices: this.pkIndices,
      syntheticPages: this.pageCount,
    };
  }

  // HOT chain methods (same interface as HeapFile)
  addHotChain(oldPageId, oldSlotIdx, newPageId, newSlotIdx) {
    this._hotChains.set(`${oldPageId}:${oldSlotIdx}`, { pageId: newPageId, slotIdx: newSlotIdx });
  }

  followHotChain(pageId, slotIdx) {
    let key = `${pageId}:${slotIdx}`;
    let current = { pageId, slotIdx };
    const visited = new Set();
    while (this._hotChains.has(key)) {
      if (visited.has(key)) break;
      visited.add(key);
      current = this._hotChains.get(key);
      key = `${current.pageId}:${current.slotIdx}`;
    }
    return current;
  }

  hasHotChain(pageId, slotIdx) {
    return this._hotChains.has(`${pageId}:${slotIdx}`);
  }

  removeHotChain(pageId, slotIdx) {
    this._hotChains.delete(`${pageId}:${slotIdx}`);
  }

  getHotChains() {
    return new Map(this._hotChains);
  }
}
