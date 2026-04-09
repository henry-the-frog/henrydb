// avl-tree.js — AVL tree: strictly balanced BST
// Height of left and right subtrees differ by at most 1.
// Stricter balance than RB tree → faster lookups, slower inserts.

class AVLNode {
  constructor(key, value) {
    this.key = key;
    this.value = value;
    this.left = null;
    this.right = null;
    this.height = 1;
  }
}

function h(n) { return n ? n.height : 0; }
function bf(n) { return h(n.left) - h(n.right); }
function fix(n) { n.height = 1 + Math.max(h(n.left), h(n.right)); }

export class AVLTree {
  constructor() { this._root = null; this._size = 0; }
  get size() { return this._size; }
  get height() { return h(this._root); }

  insert(key, value) { this._root = this._insert(this._root, key, value); }

  get(key) {
    let n = this._root;
    while (n) {
      if (key === n.key) return n.value;
      n = key < n.key ? n.left : n.right;
    }
    return undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  delete(key) { this._root = this._delete(this._root, key); }

  min() { let n = this._root; if (!n) return undefined; while (n.left) n = n.left; return { key: n.key, value: n.value }; }
  max() { let n = this._root; if (!n) return undefined; while (n.right) n = n.right; return { key: n.key, value: n.value }; }

  *[Symbol.iterator]() { yield* this._inorder(this._root); }

  _insert(n, key, value) {
    if (!n) { this._size++; return new AVLNode(key, value); }
    if (key < n.key) n.left = this._insert(n.left, key, value);
    else if (key > n.key) n.right = this._insert(n.right, key, value);
    else { n.value = value; return n; }
    return this._balance(n);
  }

  _delete(n, key) {
    if (!n) return null;
    if (key < n.key) n.left = this._delete(n.left, key);
    else if (key > n.key) n.right = this._delete(n.right, key);
    else {
      this._size--;
      if (!n.left) return n.right;
      if (!n.right) return n.left;
      let min = n.right;
      while (min.left) min = min.left;
      n.key = min.key; n.value = min.value;
      this._size++; // Undo the decrement since _delete will decrement again
      n.right = this._delete(n.right, min.key);
    }
    return this._balance(n);
  }

  _balance(n) {
    fix(n);
    const b = bf(n);
    if (b > 1) {
      if (bf(n.left) < 0) n.left = this._rotateLeft(n.left);
      return this._rotateRight(n);
    }
    if (b < -1) {
      if (bf(n.right) > 0) n.right = this._rotateRight(n.right);
      return this._rotateLeft(n);
    }
    return n;
  }

  _rotateRight(y) { const x = y.left; y.left = x.right; x.right = y; fix(y); fix(x); return x; }
  _rotateLeft(x) { const y = x.right; x.right = y.left; y.left = x; fix(x); fix(y); return y; }

  *_inorder(n) {
    if (!n) return;
    yield* this._inorder(n.left);
    yield { key: n.key, value: n.value };
    yield* this._inorder(n.right);
  }
}
