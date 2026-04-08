// heaps.js — Mergeable heap collection: Binomial, Pairing, Leftist

/**
 * Binomial Heap — mergeable priority queue.
 * Operations: insert O(log n), extractMin O(log n), merge O(log n).
 */
export class BinomialHeap {
  constructor() { this.trees = []; this._size = 0; }

  insert(key, value) {
    const h = new BinomialHeap();
    h.trees = [{ key, value, children: [], degree: 0 }];
    h._size = 1;
    this._merge(h);
    return this;
  }

  extractMin() {
    if (this.trees.length === 0) return null;
    let minIdx = 0;
    for (let i = 1; i < this.trees.length; i++) {
      if (this.trees[i].key < this.trees[minIdx].key) minIdx = i;
    }
    const min = this.trees[minIdx];
    this.trees.splice(minIdx, 1);
    
    // Children of removed tree become a new heap
    const childHeap = new BinomialHeap();
    childHeap.trees = min.children.reverse();
    childHeap._size = this._subtreeSize(childHeap.trees);
    this._size -= 1 + childHeap._size;
    this._merge(childHeap);
    
    return { key: min.key, value: min.value };
  }

  findMin() {
    if (this.trees.length === 0) return null;
    let min = this.trees[0];
    for (let i = 1; i < this.trees.length; i++) {
      if (this.trees[i].key < min.key) min = this.trees[i];
    }
    return { key: min.key, value: min.value };
  }

  _merge(other) {
    this.trees = this._mergeTrees(this.trees, other.trees);
    this._size += other._size;
  }

  _mergeTrees(a, b) {
    const merged = [...a, ...b].sort((x, y) => x.degree - y.degree);
    const result = [];
    for (let i = 0; i < merged.length; i++) {
      if (i + 1 < merged.length && merged[i].degree === merged[i + 1].degree) {
        const combined = this._link(merged[i], merged[i + 1]);
        merged[i + 1] = combined;
      } else {
        result.push(merged[i]);
      }
    }
    return result;
  }

  _link(a, b) {
    if (a.key > b.key) [a, b] = [b, a];
    a.children.push(b);
    a.degree++;
    return a;
  }

  _subtreeSize(trees) { return trees.reduce((s, t) => s + (1 << t.degree), 0); }
  get size() { return this._size; }
}

/**
 * Pairing Heap — simple self-adjusting heap.
 */
export class PairingHeap {
  constructor() { this.root = null; this._size = 0; }

  insert(key, value) {
    const node = { key, value, children: [] };
    this.root = this.root ? this._meld(this.root, node) : node;
    this._size++;
    return this;
  }

  findMin() { return this.root ? { key: this.root.key, value: this.root.value } : null; }

  extractMin() {
    if (!this.root) return null;
    const min = { key: this.root.key, value: this.root.value };
    this.root = this._mergePairs(this.root.children);
    this._size--;
    return min;
  }

  _meld(a, b) {
    if (!a) return b;
    if (!b) return a;
    if (a.key <= b.key) { a.children.push(b); return a; }
    b.children.push(a);
    return b;
  }

  _mergePairs(children) {
    if (children.length === 0) return null;
    if (children.length === 1) return children[0];
    // Two-pass pairing
    const pairs = [];
    for (let i = 0; i < children.length; i += 2) {
      if (i + 1 < children.length) pairs.push(this._meld(children[i], children[i + 1]));
      else pairs.push(children[i]);
    }
    return pairs.reduceRight((acc, h) => this._meld(acc, h));
  }

  get size() { return this._size; }
}

/**
 * Leftist Heap — merge in O(log n).
 */
export class LeftistHeap {
  constructor() { this.root = null; this._size = 0; }

  insert(key, value) {
    const node = { key, value, left: null, right: null, rank: 1 };
    this.root = this._merge(this.root, node);
    this._size++;
    return this;
  }

  findMin() { return this.root ? { key: this.root.key, value: this.root.value } : null; }

  extractMin() {
    if (!this.root) return null;
    const min = { key: this.root.key, value: this.root.value };
    this.root = this._merge(this.root.left, this.root.right);
    this._size--;
    return min;
  }

  _merge(a, b) {
    if (!a) return b;
    if (!b) return a;
    if (a.key > b.key) [a, b] = [b, a];
    a.right = this._merge(a.right, b);
    // Leftist property: rank(left) >= rank(right)
    if (this._rank(a.left) < this._rank(a.right)) [a.left, a.right] = [a.right, a.left];
    a.rank = this._rank(a.right) + 1;
    return a;
  }

  _rank(node) { return node ? node.rank : 0; }
  get size() { return this._size; }
}
