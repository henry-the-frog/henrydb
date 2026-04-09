// treap.js — Randomized BST (Tree + Heap = Treap)
// Each node has a key (BST property) and a random priority (heap property).
// Expected O(log n) for all operations without explicit balancing.
// Used in competitive programming and some database systems.

class TreapNode {
  constructor(key, value) {
    this.key = key;
    this.value = value;
    this.priority = Math.random();
    this.left = null;
    this.right = null;
    this.size = 1; // Subtree size for order statistics
  }
}

function size(node) { return node ? node.size : 0; }
function update(node) { if (node) node.size = 1 + size(node.left) + size(node.right); }

export class Treap {
  constructor() { this._root = null; }
  get size() { return size(this._root); }

  insert(key, value) {
    this._root = this._insert(this._root, key, value);
  }

  get(key) {
    let node = this._root;
    while (node) {
      if (key === node.key) return node.value;
      if (key < node.key) node = node.left;
      else node = node.right;
    }
    return undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  delete(key) {
    this._root = this._delete(this._root, key);
  }

  /** Get k-th smallest element (0-indexed). */
  kth(k) {
    return this._kth(this._root, k);
  }

  /** Get rank of key (0-indexed position in sorted order). */
  rank(key) {
    return this._rank(this._root, key);
  }

  min() {
    let node = this._root;
    if (!node) return undefined;
    while (node.left) node = node.left;
    return { key: node.key, value: node.value };
  }

  max() {
    let node = this._root;
    if (!node) return undefined;
    while (node.right) node = node.right;
    return { key: node.key, value: node.value };
  }

  /** In-order traversal. */
  *[Symbol.iterator]() {
    yield* this._inorder(this._root);
  }

  // --- Internal ---

  _rotateRight(node) {
    const left = node.left;
    node.left = left.right;
    left.right = node;
    update(node); update(left);
    return left;
  }

  _rotateLeft(node) {
    const right = node.right;
    node.right = right.left;
    right.left = node;
    update(node); update(right);
    return right;
  }

  _insert(node, key, value) {
    if (!node) return new TreapNode(key, value);
    if (key === node.key) { node.value = value; return node; }
    if (key < node.key) {
      node.left = this._insert(node.left, key, value);
      if (node.left.priority > node.priority) node = this._rotateRight(node);
    } else {
      node.right = this._insert(node.right, key, value);
      if (node.right.priority > node.priority) node = this._rotateLeft(node);
    }
    update(node);
    return node;
  }

  _delete(node, key) {
    if (!node) return null;
    if (key < node.key) node.left = this._delete(node.left, key);
    else if (key > node.key) node.right = this._delete(node.right, key);
    else {
      if (!node.left) return node.right;
      if (!node.right) return node.left;
      if (node.left.priority > node.right.priority) {
        node = this._rotateRight(node);
        node.right = this._delete(node.right, key);
      } else {
        node = this._rotateLeft(node);
        node.left = this._delete(node.left, key);
      }
    }
    update(node);
    return node;
  }

  _kth(node, k) {
    if (!node) return undefined;
    const leftSize = size(node.left);
    if (k === leftSize) return { key: node.key, value: node.value };
    if (k < leftSize) return this._kth(node.left, k);
    return this._kth(node.right, k - leftSize - 1);
  }

  _rank(node, key) {
    if (!node) return 0;
    if (key <= node.key) return this._rank(node.left, key);
    return 1 + size(node.left) + this._rank(node.right, key);
  }

  *_inorder(node) {
    if (!node) return;
    yield* this._inorder(node.left);
    yield { key: node.key, value: node.value };
    yield* this._inorder(node.right);
  }
}
