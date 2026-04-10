// merkle-tree.js — Merkle Tree for Data Synchronization
//
// Binary hash tree where each leaf is a data hash and each internal node
// is the hash of its children. Detects divergence between replicas by
// comparing root hashes, then drilling down to find differing leaves.
//
// Used in: Git (commit tree), Bitcoin (block validation), Cassandra
// (anti-entropy repair), IPFS, Certificate Transparency logs.

function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, '0');
}

function hashPair(a, b) {
  return fnv1a(a + b);
}

/**
 * MerkleTree — binary hash tree for efficient data verification.
 */
export class MerkleTree {
  constructor(data = []) {
    this._leaves = [];
    this._tree = [];
    this._data = [];
    if (data.length > 0) this.build(data);
  }

  /**
   * Build tree from an array of data items (strings or buffers).
   */
  build(data) {
    this._data = [...data];
    // Hash each leaf
    this._leaves = data.map((d, i) => fnv1a(typeof d === 'string' ? d : String(d)));
    
    // Pad to power of 2
    let leaves = [...this._leaves];
    while (leaves.length > 1 && (leaves.length & (leaves.length - 1)) !== 0) {
      leaves.push(leaves[leaves.length - 1]); // Duplicate last
    }
    
    // Build tree bottom-up
    this._tree = [leaves];
    let level = leaves;
    while (level.length > 1) {
      const next = [];
      for (let i = 0; i < level.length; i += 2) {
        const left = level[i];
        const right = i + 1 < level.length ? level[i + 1] : left;
        next.push(hashPair(left, right));
      }
      this._tree.push(next);
      level = next;
    }
  }

  /** Root hash — the single hash summarizing all data. */
  get root() {
    if (this._tree.length === 0) return null;
    const top = this._tree[this._tree.length - 1];
    return top.length > 0 ? top[0] : null;
  }

  /** Number of leaves. */
  get leafCount() { return this._leaves.length; }

  /** Get the proof (audit path) for a leaf index. */
  getProof(leafIndex) {
    const proof = [];
    let idx = leafIndex;
    
    for (let level = 0; level < this._tree.length - 1; level++) {
      const layer = this._tree[level];
      const siblingIdx = idx % 2 === 0 ? idx + 1 : idx - 1;
      const sibling = siblingIdx < layer.length ? layer[siblingIdx] : layer[idx];
      proof.push({
        hash: sibling,
        position: idx % 2 === 0 ? 'right' : 'left',
      });
      idx = Math.floor(idx / 2);
    }
    
    return proof;
  }

  /** Verify a proof for a data item at a leaf index. */
  static verifyProof(data, leafIndex, proof, root) {
    let hash = fnv1a(typeof data === 'string' ? data : String(data));
    
    for (const step of proof) {
      if (step.position === 'right') {
        hash = hashPair(hash, step.hash);
      } else {
        hash = hashPair(step.hash, hash);
      }
    }
    
    return hash === root;
  }

  /**
   * Compare with another Merkle tree and find differing leaf indices.
   * Efficient: only descends into branches where hashes differ.
   */
  diff(other) {
    if (this.root === other.root) return [];
    
    const diffs = [];
    const maxLeaf = Math.min(this._leaves.length, other._leaves.length);
    this._diffLevel(other, this._tree.length - 1, 0, diffs, maxLeaf);
    return diffs;
  }

  _diffLevel(other, level, offset, diffs, maxLeaf) {
    if (level < 0) return;
    
    const myLayer = this._tree[level] || [];
    const otherLayer = other._tree[level] || [];
    
    if (level === 0) {
      // Leaf level: report actual differences
      if (offset >= maxLeaf) return; // Skip padding
      if (offset < myLayer.length && offset < otherLayer.length) {
        if (myLayer[offset] !== otherLayer[offset]) {
          diffs.push(offset);
        }
      } else if (offset < myLayer.length || offset < otherLayer.length) {
        diffs.push(offset);
      }
      return;
    }
    
    const myHash = offset < myLayer.length ? myLayer[offset] : null;
    const otherHash = offset < otherLayer.length ? otherLayer[offset] : null;
    
    if (myHash === otherHash) return; // Subtree matches
    
    // Recurse into children
    this._diffLevel(other, level - 1, offset * 2, diffs, maxLeaf);
    this._diffLevel(other, level - 1, offset * 2 + 1, diffs, maxLeaf);
  }
}
