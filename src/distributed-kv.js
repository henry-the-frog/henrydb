// distributed-kv.js — Distributed key-value store concept
// Integrates: SHA-256, consistent hashing, Bloom filter, Merkle tree, MVCC
//
// Architecture:
// - Multiple partitions arranged on a consistent hashing ring
// - Each partition has a Bloom filter for fast existence checks
// - Each partition has a Merkle tree for anti-entropy sync
// - MVCC provides snapshot isolation within each partition
// - Replication factor N means each key lives on N consecutive partitions

import { sha256 } from './sha256.js';
import { ConsistentHashRing } from './consistent-hash.js';
import { BloomFilter } from './bloom.js';
import { MerkleTree } from './merkle.js';
import { MVCCManager } from './mvcc.js';

/**
 * Partition — a single shard in the distributed KV store.
 * Has its own MVCC manager, Bloom filter, and Merkle tree.
 */
class Partition {
  constructor(id) {
    this.id = id;
    this._data = new Map();         // key → value (latest committed)
    this._mvcc = new MVCCManager();  // Transaction management
    this._bloom = new BloomFilter(10000, 0.01); // Existence filter
    this._merkleData = [];           // Data blocks for Merkle tree
    this._merkle = null;             // Rebuilt on demand
    this._dirty = true;              // Merkle tree needs rebuild
  }

  /** Get a value by key (auto-commit read). */
  get(key) {
    return this._data.get(key) ?? null;
  }

  /** Set a key-value pair (auto-commit write). */
  set(key, value) {
    this._data.set(key, value);
    this._bloom.add(key);
    this._dirty = true;
  }

  /** Delete a key. */
  delete(key) {
    this._data.delete(key);
    this._dirty = true;
  }

  /** Check if key might exist (Bloom filter — fast, no false negatives). */
  mightContain(key) {
    return this._bloom.test(key);
  }

  /** Get the Merkle root for anti-entropy comparison. */
  getMerkleRoot() {
    this._rebuildMerkle();
    return this._merkle?.root ?? sha256('empty');
  }

  /** Get keys that differ from another partition's Merkle tree. */
  diff(other) {
    this._rebuildMerkle();
    other._rebuildMerkle();
    if (!this._merkle || !other._merkle) return [];
    if (this._merkle.leafCount !== other._merkle.leafCount) {
      // Different number of keys — full sync needed
      return [...new Set([...this._data.keys(), ...other._data.keys()])];
    }
    return this._merkle.diff(other._merkle);
  }

  /** Begin a transactional read/write session. */
  beginTransaction(options) {
    return this._mvcc.begin(options);
  }

  /** Read within a transaction. */
  txGet(tx, key) {
    // Check MVCC first (for in-flight changes)
    const mvccVal = this._mvcc.read(tx, key);
    if (mvccVal !== undefined) return mvccVal;
    // Fall back to committed data
    return this._data.get(key) ?? null;
  }

  /** Write within a transaction. */
  txSet(tx, key, value) {
    this._mvcc.write(tx, key, value);
  }

  /** Commit: apply MVCC writes to the data store. */
  txCommit(tx) {
    // Apply writes to committed data
    for (const key of tx.writeSet) {
      const val = this._mvcc.read(tx, key);
      if (val !== undefined) {
        this._data.set(key, val);
        this._bloom.add(key);
      }
    }
    this._mvcc.commit(tx);
    this._dirty = true;
  }

  /** Rollback transaction. */
  txRollback(tx) {
    this._mvcc.rollback(tx);
  }

  /** Get partition stats. */
  getStats() {
    return {
      id: this.id,
      keys: this._data.size,
      bloomFPR: this._bloom.estimateFPR().toFixed(4),
      merkleRoot: this.getMerkleRoot().slice(0, 16) + '...',
      mvccStats: this._mvcc.getStats(),
    };
  }

  // Rebuild Merkle tree from current data
  _rebuildMerkle() {
    if (!this._dirty) return;
    const entries = [...this._data.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${JSON.stringify(v)}`);
    
    if (entries.length === 0) {
      this._merkle = null;
    } else {
      this._merkle = new MerkleTree(entries);
    }
    this._dirty = false;
  }
}

/**
 * DistributedKV — the main distributed key-value store.
 * Manages partitions on a consistent hashing ring.
 */
export class DistributedKV {
  /**
   * @param {Object} options
   * @param {number} [options.replicationFactor=3] — number of replicas per key
   * @param {number} [options.virtualNodes=150] — vnodes per partition
   */
  constructor(options = {}) {
    this._replicationFactor = options.replicationFactor ?? 3;
    this._ring = new ConsistentHashRing(options.virtualNodes ?? 150);
    this._partitions = new Map(); // partitionId → Partition
  }

  /** Add a partition (node) to the cluster. */
  addPartition(id) {
    if (this._partitions.has(id)) return;
    this._partitions.set(id, new Partition(id));
    this._ring.addNode(id);
  }

  /** Remove a partition from the cluster. */
  removePartition(id) {
    this._partitions.delete(id);
    this._ring.removeNode(id);
  }

  /** Get the partitions responsible for a key. */
  getPartitionsForKey(key) {
    const nodeIds = this._ring.getNodes(key, this._replicationFactor);
    return nodeIds.map(id => this._partitions.get(id)).filter(Boolean);
  }

  /**
   * Put a key-value pair (writes to all replicas).
   * @param {string} key
   * @param {*} value
   */
  put(key, value) {
    const partitions = this.getPartitionsForKey(key);
    for (const p of partitions) {
      p.set(key, value);
    }
    return { replicas: partitions.length };
  }

  /**
   * Get a value by key (reads from first available replica).
   * Uses Bloom filter for fast "definitely not here" check.
   * @param {string} key
   * @returns {*}
   */
  get(key) {
    const partitions = this.getPartitionsForKey(key);
    for (const p of partitions) {
      // Fast Bloom filter check
      if (!p.mightContain(key)) continue;
      const val = p.get(key);
      if (val !== null) return val;
    }
    return null;
  }

  /**
   * Delete a key from all replicas.
   */
  delete(key) {
    const partitions = this.getPartitionsForKey(key);
    for (const p of partitions) p.delete(key);
  }

  /**
   * Run anti-entropy sync between two partitions.
   * Uses Merkle trees to efficiently find differences.
   * @returns {{ synced: number }}
   */
  antiEntropy(partitionA, partitionB) {
    const a = this._partitions.get(partitionA);
    const b = this._partitions.get(partitionB);
    if (!a || !b) throw new Error('Partition not found');
    
    // Compare Merkle roots
    if (a.getMerkleRoot() === b.getMerkleRoot()) {
      return { synced: 0 }; // Already in sync
    }
    
    // Find differences and sync
    let synced = 0;
    for (const [key, val] of a._data) {
      if (b.get(key) !== val) {
        b.set(key, val);
        synced++;
      }
    }
    for (const [key, val] of b._data) {
      if (a.get(key) !== val) {
        a.set(key, val);
        synced++;
      }
    }
    
    return { synced };
  }

  /**
   * Execute a transaction across partitions.
   * @param {function} fn — receives a transaction context
   */
  transaction(fn) {
    // Start transactions on all partitions
    const txns = new Map();
    for (const [id, p] of this._partitions) {
      txns.set(id, p.beginTransaction());
    }
    
    const ctx = {
      get: (key) => {
        const partitions = this.getPartitionsForKey(key);
        for (const p of partitions) {
          const tx = txns.get(p.id);
          const val = p.txGet(tx, key);
          if (val !== null) return val;
        }
        return null;
      },
      put: (key, value) => {
        const partitions = this.getPartitionsForKey(key);
        for (const p of partitions) {
          const tx = txns.get(p.id);
          p.txSet(tx, key, value);
        }
      },
    };
    
    try {
      fn(ctx);
      // Commit all
      for (const [id, tx] of txns) {
        this._partitions.get(id).txCommit(tx);
      }
      return { committed: true };
    } catch (e) {
      // Rollback all
      for (const [id, tx] of txns) {
        this._partitions.get(id).txRollback(tx);
      }
      return { committed: false, error: e.message };
    }
  }

  /** Get cluster-wide stats. */
  getStats() {
    const stats = {
      partitions: this._partitions.size,
      replicationFactor: this._replicationFactor,
      partitionStats: [],
    };
    for (const [, p] of this._partitions) {
      stats.partitionStats.push(p.getStats());
    }
    return stats;
  }
}
