// mvcc-skip-list.js — Skip List with Multi-Version Concurrency Control
//
// Combines a lock-free-style skip list with MVCC version chains.
// Each key has a chain of versions, each tagged with a txId.
// Readers see a snapshot-consistent view based on their transaction.
//
// Used in: CockroachDB (Pebble memtable), BadgerDB, many in-memory MVCC stores.
//
// Key insight: The skip list provides O(log n) ordered access.
// The version chain on each node provides snapshot isolation.

const MAX_LEVEL = 16;

function randomLevel() {
  let level = 1;
  while (level < MAX_LEVEL && Math.random() < 0.5) level++;
  return level;
}

/**
 * Version entry — one version of a value at a key.
 */
class Version {
  constructor(txId, value, deleted = false) {
    this.txId = txId;
    this.value = value;
    this.deleted = deleted;    // Tombstone
    this.committed = false;
    this.next = null;          // Older version (linked list: newest → oldest)
  }
}

/**
 * Skip list node — key with a version chain.
 */
class Node {
  constructor(key, level) {
    this.key = key;
    this.forward = new Array(level).fill(null);
    this.versions = null;  // Head of version chain (newest first)
  }
}

/**
 * MVCCSkipList — ordered key-value store with snapshot isolation.
 */
export class MVCCSkipList {
  constructor() {
    this._header = new Node(null, MAX_LEVEL);
    this._level = 1;
    this._size = 0;
    this._nextTxId = 1;
    this._activeTxns = new Map();
    this._committedTxns = new Set();
  }

  get size() { return this._size; }

  // ============================================================
  // Transaction management
  // ============================================================

  /**
   * Begin a new transaction. Returns a transaction handle.
   */
  begin() {
    const txId = this._nextTxId++;
    // Take snapshot: record active (uncommitted) txids
    const activeSet = new Set();
    for (const [id, tx] of this._activeTxns) {
      if (!tx.committed) activeSet.add(id);
    }
    const tx = {
      txId,
      snapshot: { xmin: this._computeXmin(), xmax: this._nextTxId, activeSet },
      committed: false,
      writeSet: new Set(),
    };
    this._activeTxns.set(txId, tx);
    return tx;
  }

  /**
   * Commit a transaction.
   */
  commit(tx) {
    tx.committed = true;
    this._committedTxns.add(tx.txId);
    // Mark all versions written by this tx as committed
    for (const key of tx.writeSet) {
      const node = this._findNode(key);
      if (node) {
        let ver = node.versions;
        while (ver) {
          if (ver.txId === tx.txId) ver.committed = true;
          ver = ver.next;
        }
      }
    }
  }

  /**
   * Rollback a transaction — remove all versions written by it.
   */
  rollback(tx) {
    for (const key of tx.writeSet) {
      const node = this._findNode(key);
      if (node) {
        // Remove versions belonging to this tx
        let prev = null;
        let ver = node.versions;
        while (ver) {
          if (ver.txId === tx.txId) {
            if (prev) prev.next = ver.next;
            else node.versions = ver.next;
          } else {
            prev = ver;
          }
          ver = ver.next;
        }
      }
    }
    this._activeTxns.delete(tx.txId);
  }

  // ============================================================
  // Read/Write operations
  // ============================================================

  /**
   * Put a key-value pair within a transaction.
   */
  put(tx, key, value) {
    let node = this._findNode(key);
    if (!node) {
      node = this._insertNode(key);
      this._size++;
    }
    // Add new version at head of chain
    const ver = new Version(tx.txId, value, false);
    ver.next = node.versions;
    node.versions = ver;
    tx.writeSet.add(key);
  }

  /**
   * Delete a key within a transaction (tombstone).
   */
  delete(tx, key) {
    let node = this._findNode(key);
    if (!node) {
      node = this._insertNode(key);
      this._size++;
    }
    const ver = new Version(tx.txId, null, true);
    ver.next = node.versions;
    node.versions = ver;
    tx.writeSet.add(key);
  }

  /**
   * Get a value by key, respecting the transaction's snapshot.
   * Returns undefined if not found or deleted in this snapshot.
   */
  get(tx, key) {
    const node = this._findNode(key);
    if (!node) return undefined;
    
    const ver = this._visibleVersion(node, tx);
    if (!ver || ver.deleted) return undefined;
    return ver.value;
  }

  /**
   * Scan keys in order within a range, respecting snapshot isolation.
   * Yields {key, value} pairs.
   */
  *scan(tx, startKey = null, endKey = null) {
    let node = this._header.forward[0];
    
    // Skip to startKey
    if (startKey !== null) {
      node = this._findFirstGE(startKey);
    }
    
    while (node) {
      if (endKey !== null && node.key > endKey) break;
      
      const ver = this._visibleVersion(node, tx);
      if (ver && !ver.deleted) {
        yield { key: node.key, value: ver.value };
      }
      
      node = node.forward[0];
    }
  }

  /**
   * Count keys visible to a transaction in a range.
   */
  count(tx, startKey = null, endKey = null) {
    let count = 0;
    for (const _ of this.scan(tx, startKey, endKey)) count++;
    return count;
  }

  // ============================================================
  // MVCC Visibility
  // ============================================================

  /**
   * Find the latest version visible to this transaction.
   * Uses PostgreSQL-style snapshot rules.
   */
  _visibleVersion(node, tx) {
    let ver = node.versions;
    while (ver) {
      if (this._isVisible(ver, tx)) return ver;
      ver = ver.next;
    }
    return null;
  }

  /**
   * Check if a version is visible to a transaction.
   * Rules:
   * - Own writes are always visible
   * - Committed versions below snapshot.xmin are visible
   * - Committed versions in [xmin, xmax) not in activeSet are visible
   * - Everything else is invisible
   */
  _isVisible(ver, tx) {
    if (ver.txId === tx.txId) return true;
    
    const snap = tx.snapshot;
    
    // Below xmin: committed before any active tx
    if (ver.txId < snap.xmin) {
      return ver.committed || this._committedTxns.has(ver.txId);
    }
    
    // At or above xmax: started after snapshot
    if (ver.txId >= snap.xmax) return false;
    
    // In active set: was in-progress at snapshot time
    if (snap.activeSet.has(ver.txId)) return false;
    
    // Between xmin and xmax, not in active set: must be committed
    return ver.committed || this._committedTxns.has(ver.txId);
  }

  _computeXmin() {
    let min = this._nextTxId;
    for (const [id, tx] of this._activeTxns) {
      if (!tx.committed && id < min) min = id;
    }
    return min;
  }

  // ============================================================
  // Skip List Operations
  // ============================================================

  _findNode(key) {
    let current = this._header;
    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] && current.forward[i].key < key) {
        current = current.forward[i];
      }
    }
    current = current.forward[0];
    if (current && current.key === key) return current;
    return null;
  }

  _findFirstGE(key) {
    let current = this._header;
    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] && current.forward[i].key < key) {
        current = current.forward[i];
      }
    }
    return current.forward[0];
  }

  _insertNode(key) {
    const update = new Array(MAX_LEVEL).fill(null);
    let current = this._header;
    
    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] && current.forward[i].key < key) {
        current = current.forward[i];
      }
      update[i] = current;
    }
    
    // Check if key already exists
    if (current.forward[0] && current.forward[0].key === key) {
      return current.forward[0];
    }
    
    const newLevel = randomLevel();
    if (newLevel > this._level) {
      for (let i = this._level; i < newLevel; i++) {
        update[i] = this._header;
      }
      this._level = newLevel;
    }
    
    const newNode = new Node(key, newLevel);
    for (let i = 0; i < newLevel; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;
    }
    
    return newNode;
  }

  /**
   * Garbage collect old versions not needed by any active transaction.
   */
  gc() {
    const xmin = this._computeXmin();
    let cleaned = 0;
    
    let node = this._header.forward[0];
    while (node) {
      let ver = node.versions;
      let prev = null;
      let foundVisible = false;
      
      while (ver) {
        if (foundVisible && ver.committed && ver.txId < xmin) {
          // This version and all older ones are not needed
          if (prev) prev.next = null;
          cleaned++;
          break;
        }
        if (ver.committed) foundVisible = true;
        prev = ver;
        ver = ver.next;
      }
      
      node = node.forward[0];
    }
    
    return cleaned;
  }

  /**
   * Statistics about the skip list and version chains.
   */
  getStats() {
    let totalVersions = 0;
    let maxVersions = 0;
    let tombstones = 0;
    let nodes = 0;
    
    let node = this._header.forward[0];
    while (node) {
      nodes++;
      let count = 0;
      let ver = node.versions;
      while (ver) {
        count++;
        if (ver.deleted) tombstones++;
        ver = ver.next;
      }
      totalVersions += count;
      if (count > maxVersions) maxVersions = count;
      node = node.forward[0];
    }
    
    return {
      nodes,
      totalVersions,
      maxVersionChainLength: maxVersions,
      tombstones,
      activeTxns: this._activeTxns.size,
      level: this._level,
    };
  }
}
