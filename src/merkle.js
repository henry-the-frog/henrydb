// merkle.js — Merkle tree for data integrity verification
// Uses our SHA-256 implementation for hashing.

import { sha256 } from './sha256.js';

/**
 * MerkleTree — binary hash tree for efficient integrity proofs.
 * 
 * Each leaf is the hash of a data block. Internal nodes are the hash
 * of their two children concatenated. If the number of leaves is odd,
 * the last leaf is duplicated.
 * 
 * Used in: Bitcoin, Git, IPFS, database page checksum verification.
 */
export class MerkleTree {
  /**
   * Build a Merkle tree from data blocks.
   * @param {string[]} blocks — array of data strings to hash
   */
  constructor(blocks) {
    if (blocks.length === 0) throw new Error('Cannot build Merkle tree from empty data');
    
    // Hash each block to create leaves
    this._leaves = blocks.map((b, i) => ({
      hash: sha256(b),
      data: b,
      index: i,
    }));
    
    // Build tree bottom-up
    this._layers = [this._leaves.map(l => l.hash)];
    let current = this._layers[0];
    
    while (current.length > 1) {
      const next = [];
      for (let i = 0; i < current.length; i += 2) {
        const left = current[i];
        const right = i + 1 < current.length ? current[i + 1] : current[i]; // duplicate if odd
        next.push(sha256(left + right));
      }
      this._layers.push(next);
      current = next;
    }
    
    this._root = current[0];
  }

  /** Get the Merkle root hash. */
  get root() { return this._root; }
  
  /** Get the number of leaves. */
  get leafCount() { return this._leaves.length; }
  
  /** Get all layers (bottom to top). */
  get layers() { return this._layers; }

  /**
   * Generate a Merkle proof for a leaf at the given index.
   * Returns an array of {hash, direction} pairs (the "audit path").
   * @param {number} index — leaf index (0-based)
   * @returns {{ hash: string, direction: 'left'|'right' }[]}
   */
  getProof(index) {
    if (index < 0 || index >= this._leaves.length) {
      throw new Error(`Index ${index} out of range [0, ${this._leaves.length})`);
    }
    
    const proof = [];
    let idx = index;
    
    for (let layer = 0; layer < this._layers.length - 1; layer++) {
      const isRight = idx % 2 === 1;
      const siblingIdx = isRight ? idx - 1 : idx + 1;
      
      // If sibling doesn't exist (odd count), use self (duplicate)
      const siblingHash = siblingIdx < this._layers[layer].length
        ? this._layers[layer][siblingIdx]
        : this._layers[layer][idx];
      
      proof.push({
        hash: siblingHash,
        direction: isRight ? 'left' : 'right',
      });
      
      idx = Math.floor(idx / 2);
    }
    
    return proof;
  }

  /**
   * Verify a Merkle proof.
   * @param {string} leafData — the original data
   * @param {{ hash: string, direction: string }[]} proof — the audit path
   * @param {string} root — the expected Merkle root
   * @returns {boolean}
   */
  static verify(leafData, proof, root) {
    let hash = sha256(leafData);
    
    for (const step of proof) {
      if (step.direction === 'left') {
        hash = sha256(step.hash + hash);
      } else {
        hash = sha256(hash + step.hash);
      }
    }
    
    return hash === root;
  }

  /**
   * Check if two trees have the same root (fast equality check).
   * @param {MerkleTree} other
   * @returns {boolean}
   */
  equals(other) {
    return this._root === other._root;
  }

  /**
   * Find which leaves differ between two trees of the same size.
   * Uses top-down comparison to efficiently identify changed blocks.
   * @param {MerkleTree} other
   * @returns {number[]} — indices of differing leaves
   */
  diff(other) {
    if (this._leaves.length !== other._leaves.length) {
      throw new Error('Trees must have same number of leaves for diff');
    }
    if (this._root === other._root) return []; // identical
    
    const diffs = [];
    this._diffRecursive(other, this._layers.length - 1, 0, diffs);
    return diffs;
  }

  _diffRecursive(other, layer, nodeIdx, diffs) {
    if (layer === 0) {
      // Leaf layer: check if this leaf differs
      if (nodeIdx < this._leaves.length && 
          this._layers[0][nodeIdx] !== other._layers[0][nodeIdx]) {
        diffs.push(nodeIdx);
      }
      return;
    }
    
    // Compare this node's hash with other's
    if (this._layers[layer][nodeIdx] === other._layers[layer]?.[nodeIdx]) {
      return; // Subtrees match — skip
    }
    
    // Recurse into children
    const leftChild = nodeIdx * 2;
    const rightChild = nodeIdx * 2 + 1;
    
    this._diffRecursive(other, layer - 1, leftChild, diffs);
    if (rightChild < this._layers[layer - 1].length) {
      this._diffRecursive(other, layer - 1, rightChild, diffs);
    }
  }
}
