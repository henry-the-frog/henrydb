// red-black-tree.js — Left-leaning Red-Black BST (Sedgewick)
// Simplified RB tree: red links lean left. Guarantees O(log n).
// Used in: Java TreeMap, C++ std::map, Linux kernel.

const RED = true, BLACK = false;

class RBNode {
  constructor(key, value, color) {
    this.key = key;
    this.value = value;
    this.color = color;
    this.left = null;
    this.right = null;
    this.size = 1;
  }
}

function isRed(n) { return n ? n.color === RED : false; }
function sz(n) { return n ? n.size : 0; }

export class RedBlackTree {
  constructor() { this._root = null; }
  get size() { return sz(this._root); }

  insert(key, value) { this._root = this._put(this._root, key, value); this._root.color = BLACK; }

  get(key) {
    let n = this._root;
    while (n) {
      if (key === n.key) return n.value;
      if (key < n.key) n = n.left;
      else n = n.right;
    }
    return undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  min() {
    if (!this._root) return undefined;
    let n = this._root;
    while (n.left) n = n.left;
    return { key: n.key, value: n.value };
  }

  max() {
    if (!this._root) return undefined;
    let n = this._root;
    while (n.right) n = n.right;
    return { key: n.key, value: n.value };
  }

  /** Height of tree (black height). */
  height() { return this._height(this._root); }

  *[Symbol.iterator]() { yield* this._inorder(this._root); }

  _put(h, key, value) {
    if (!h) return new RBNode(key, value, RED);
    if (key < h.key) h.left = this._put(h.left, key, value);
    else if (key > h.key) h.right = this._put(h.right, key, value);
    else h.value = value;

    // Fix-up
    if (isRed(h.right) && !isRed(h.left)) h = this._rotateLeft(h);
    if (isRed(h.left) && isRed(h.left?.left)) h = this._rotateRight(h);
    if (isRed(h.left) && isRed(h.right)) this._flipColors(h);

    h.size = 1 + sz(h.left) + sz(h.right);
    return h;
  }

  _rotateLeft(h) {
    const x = h.right;
    h.right = x.left;
    x.left = h;
    x.color = h.color;
    h.color = RED;
    x.size = h.size;
    h.size = 1 + sz(h.left) + sz(h.right);
    return x;
  }

  _rotateRight(h) {
    const x = h.left;
    h.left = x.right;
    x.right = h;
    x.color = h.color;
    h.color = RED;
    x.size = h.size;
    h.size = 1 + sz(h.left) + sz(h.right);
    return x;
  }

  _flipColors(h) {
    h.color = RED;
    h.left.color = BLACK;
    h.right.color = BLACK;
  }

  _height(n) {
    if (!n) return 0;
    return 1 + Math.max(this._height(n.left), this._height(n.right));
  }

  *_inorder(n) {
    if (!n) return;
    yield* this._inorder(n.left);
    yield { key: n.key, value: n.value };
    yield* this._inorder(n.right);
  }
}
