// more-trees2.js — Scapegoat tree + Weight-balanced tree

/**
 * Scapegoat Tree — balanced BST with lazy rebalancing.
 * After insert, if depth > log_α(n), find a "scapegoat" node to rebuild.
 */
export class ScapegoatTree {
  constructor(alpha = 0.7) {
    this.alpha = alpha;
    this.root = null;
    this._size = 0;
    this._maxSize = 0;
  }

  insert(key, value) {
    const [newRoot, depth] = this._insert(this.root, key, value, 0);
    this.root = newRoot;
    if (depth > this._logAlpha(this._size)) {
      // Find scapegoat and rebuild
      this.root = this._rebuild(this.root);
    }
  }

  _insert(node, key, value, depth) {
    if (!node) {
      this._size++;
      this._maxSize = Math.max(this._maxSize, this._size);
      return [{ key, value, left: null, right: null, size: 1 }, depth];
    }
    if (key < node.key) {
      const [left, d] = this._insert(node.left, key, value, depth + 1);
      node.left = left;
      node.size = 1 + this._sizeOf(node.left) + this._sizeOf(node.right);
      return [node, d];
    } else if (key > node.key) {
      const [right, d] = this._insert(node.right, key, value, depth + 1);
      node.right = right;
      node.size = 1 + this._sizeOf(node.left) + this._sizeOf(node.right);
      return [node, d];
    } else {
      node.value = value;
      return [node, depth];
    }
  }

  search(key) {
    let node = this.root;
    while (node) {
      if (key < node.key) node = node.left;
      else if (key > node.key) node = node.right;
      else return node.value;
    }
    return undefined;
  }

  _rebuild(node) {
    const sorted = [];
    this._flatten(node, sorted);
    return this._buildBalanced(sorted, 0, sorted.length - 1);
  }

  _flatten(node, arr) {
    if (!node) return;
    this._flatten(node.left, arr);
    arr.push(node);
    this._flatten(node.right, arr);
  }

  _buildBalanced(arr, lo, hi) {
    if (lo > hi) return null;
    const mid = (lo + hi) >>> 1;
    const node = arr[mid];
    node.left = this._buildBalanced(arr, lo, mid - 1);
    node.right = this._buildBalanced(arr, mid + 1, hi);
    node.size = 1 + this._sizeOf(node.left) + this._sizeOf(node.right);
    return node;
  }

  _sizeOf(node) { return node ? node.size : 0; }
  _logAlpha(n) { return Math.floor(Math.log(n) / Math.log(1 / this.alpha)); }
  get size() { return this._size; }
}

/**
 * Weight-balanced Tree (BB[α] tree) — balanced by subtree weight.
 */
export class WeightBalancedTree {
  constructor(alpha = 0.29) {
    this.alpha = alpha;
    this.root = null;
    this._size = 0;
  }

  insert(key, value) {
    this.root = this._insert(this.root, key, value);
  }

  _insert(node, key, value) {
    if (!node) { this._size++; return { key, value, left: null, right: null, weight: 1 }; }
    if (key < node.key) node.left = this._insert(node.left, key, value);
    else if (key > node.key) node.right = this._insert(node.right, key, value);
    else { node.value = value; return node; }
    
    node.weight = 1 + this._w(node.left) + this._w(node.right);
    return this._balance(node);
  }

  _balance(node) {
    const total = node.weight;
    if (this._w(node.left) > this.alpha * total + 1) {
      // Left-heavy
      if (this._w(node.left?.left) > this._w(node.left?.right)) {
        return this._rotateRight(node);
      } else {
        node.left = this._rotateLeft(node.left);
        return this._rotateRight(node);
      }
    }
    if (this._w(node.right) > this.alpha * total + 1) {
      if (this._w(node.right?.right) > this._w(node.right?.left)) {
        return this._rotateLeft(node);
      } else {
        node.right = this._rotateRight(node.right);
        return this._rotateLeft(node);
      }
    }
    return node;
  }

  _rotateRight(h) {
    const x = h.left;
    h.left = x.right;
    x.right = h;
    h.weight = 1 + this._w(h.left) + this._w(h.right);
    x.weight = 1 + this._w(x.left) + this._w(x.right);
    return x;
  }

  _rotateLeft(h) {
    const x = h.right;
    h.right = x.left;
    x.left = h;
    h.weight = 1 + this._w(h.left) + this._w(h.right);
    x.weight = 1 + this._w(x.left) + this._w(x.right);
    return x;
  }

  _w(node) { return node ? node.weight : 0; }

  search(key) {
    let node = this.root;
    while (node) {
      if (key < node.key) node = node.left;
      else if (key > node.key) node = node.right;
      else return node.value;
    }
    return undefined;
  }

  *inOrder(node = this.root) {
    if (!node) return;
    yield* this.inOrder(node.left);
    yield { key: node.key, value: node.value };
    yield* this.inOrder(node.right);
  }

  get size() { return this._size; }
}
