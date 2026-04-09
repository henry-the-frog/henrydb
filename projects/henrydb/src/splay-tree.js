// splay-tree.js — Self-adjusting BST
// Recently accessed elements move to root via splaying (zig/zig-zig/zig-zag rotations).
// Amortized O(log n) for all operations. Optimal for non-uniform access patterns.
// Used in: gcc allocator, Windows NT, network routers.

class SplayNode {
  constructor(key, value) {
    this.key = key;
    this.value = value;
    this.left = null;
    this.right = null;
  }
}

export class SplayTree {
  constructor() { this._root = null; this._size = 0; }
  get size() { return this._size; }

  insert(key, value) {
    if (!this._root) { this._root = new SplayNode(key, value); this._size++; return; }
    this._root = this._splay(this._root, key);
    if (this._root.key === key) { this._root.value = value; return; }
    
    const node = new SplayNode(key, value);
    if (key < this._root.key) {
      node.right = this._root;
      node.left = this._root.left;
      this._root.left = null;
    } else {
      node.left = this._root;
      node.right = this._root.right;
      this._root.right = null;
    }
    this._root = node;
    this._size++;
  }

  get(key) {
    if (!this._root) return undefined;
    this._root = this._splay(this._root, key);
    return this._root.key === key ? this._root.value : undefined;
  }

  has(key) {
    if (!this._root) return false;
    this._root = this._splay(this._root, key);
    return this._root.key === key;
  }

  delete(key) {
    if (!this._root) return false;
    this._root = this._splay(this._root, key);
    if (this._root.key !== key) return false;
    
    if (!this._root.left) {
      this._root = this._root.right;
    } else {
      const right = this._root.right;
      this._root = this._splay(this._root.left, key);
      this._root.right = right;
    }
    this._size--;
    return true;
  }

  min() {
    if (!this._root) return undefined;
    let n = this._root;
    while (n.left) n = n.left;
    this._root = this._splay(this._root, n.key);
    return { key: n.key, value: n.value };
  }

  max() {
    if (!this._root) return undefined;
    let n = this._root;
    while (n.right) n = n.right;
    this._root = this._splay(this._root, n.key);
    return { key: n.key, value: n.value };
  }

  *[Symbol.iterator]() { yield* this._inorder(this._root); }

  _splay(node, key) {
    if (!node) return node;
    if (key < node.key) {
      if (!node.left) return node;
      if (key < node.left.key) {
        node.left.left = this._splay(node.left.left, key);
        node = this._rotateRight(node);
      } else if (key > node.left.key) {
        node.left.right = this._splay(node.left.right, key);
        if (node.left.right) node.left = this._rotateLeft(node.left);
      }
      return node.left ? this._rotateRight(node) : node;
    } else if (key > node.key) {
      if (!node.right) return node;
      if (key > node.right.key) {
        node.right.right = this._splay(node.right.right, key);
        node = this._rotateLeft(node);
      } else if (key < node.right.key) {
        node.right.left = this._splay(node.right.left, key);
        if (node.right.left) node.right = this._rotateRight(node.right);
      }
      return node.right ? this._rotateLeft(node) : node;
    }
    return node;
  }

  _rotateRight(x) { const y = x.left; x.left = y.right; y.right = x; return y; }
  _rotateLeft(x) { const y = x.right; x.right = y.left; y.left = x; return y; }

  *_inorder(node) {
    if (!node) return;
    yield* this._inorder(node.left);
    yield { key: node.key, value: node.value };
    yield* this._inorder(node.right);
  }
}
