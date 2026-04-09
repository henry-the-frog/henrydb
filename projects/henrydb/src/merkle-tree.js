// merkle-tree.js — Hash tree for data integrity (Git, blockchain, IPFS)
// Leaf nodes contain data hashes. Internal nodes hash their children.
// Efficient verification: O(log n) proof for any leaf.

import { createHash } from 'node:crypto';

function sha256(data) { return createHash('sha256').update(data).digest('hex'); }

class MerkleNode {
  constructor(hash, left = null, right = null) {
    this.hash = hash;
    this.left = left;
    this.right = right;
  }
}

export class MerkleTree {
  constructor(data) {
    if (!data || data.length === 0) { this._root = null; return; }
    const leaves = data.map(d => new MerkleNode(sha256(String(d))));
    this._root = this._build(leaves);
    this._leaves = leaves;
    this._data = data;
  }

  get root() { return this._root ? this._root.hash : null; }

  /** Verify that data at index hasn't been tampered with. */
  verify(index) {
    if (!this._root || index >= this._data.length) return false;
    const expectedHash = sha256(String(this._data[index]));
    return this._leaves[index].hash === expectedHash;
  }

  /** Get proof path for a leaf (for independent verification). */
  getProof(index) {
    const proof = [];
    this._getProof(this._root, index, 0, this._leaves.length, proof);
    return proof;
  }

  /** Verify a proof independently. */
  static verifyProof(leaf, proof, root) {
    let hash = sha256(String(leaf));
    for (const { hash: siblingHash, position } of proof) {
      if (position === 'left') hash = sha256(siblingHash + hash);
      else hash = sha256(hash + siblingHash);
    }
    return hash === root;
  }

  _build(nodes) {
    if (nodes.length === 1) return nodes[0];
    const next = [];
    for (let i = 0; i < nodes.length; i += 2) {
      const left = nodes[i];
      const right = nodes[i + 1] || left; // Duplicate last if odd
      const hash = sha256(left.hash + right.hash);
      next.push(new MerkleNode(hash, left, right));
    }
    return this._build(next);
  }

  _getProof(node, index, start, end, proof) {
    if (end - start <= 1) return;
    const mid = Math.ceil((start + end) / 2);
    if (index < mid) {
      // Go left, sibling is right
      if (node.right) proof.push({ hash: node.right.hash, position: 'right' });
      this._getProof(node.left, index, start, mid, proof);
    } else {
      // Go right, sibling is left
      if (node.left) proof.push({ hash: node.left.hash, position: 'left' });
      this._getProof(node.right, index, mid, end, proof);
    }
  }
}
