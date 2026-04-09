// fibonacci-heap.js — Simplified Fibonacci heap
// O(1) amortized insert + decrease-key, O(log n) amortized extract-min.
// Key structure for Dijkstra's shortest path (improves from O(V log V) to O(V + E log V)).
// Note: This is a simplified version; full impl needs consolidation + cascading cuts.

class FibNode {
  constructor(key, value) {
    this.key = key;
    this.value = value;
    this.degree = 0;
    this.mark = false;
    this.parent = null;
    this.child = null;
    this.left = this;
    this.right = this;
  }
}

function insertIntoList(list, node) {
  if (!list) { node.left = node; node.right = node; return node; }
  node.right = list;
  node.left = list.left;
  list.left.right = node;
  list.left = node;
  return list;
}

function removeFromList(node) {
  if (node.right === node) return null;
  node.left.right = node.right;
  node.right.left = node.left;
  const next = node.right;
  node.left = node;
  node.right = node;
  return next;
}

export class FibonacciHeap {
  constructor() {
    this._min = null;
    this._size = 0;
    this._nodes = new Map(); // value → node for decrease-key
  }

  get size() { return this._size; }
  get isEmpty() { return this._size === 0; }

  /** Insert. O(1). */
  insert(key, value) {
    const node = new FibNode(key, value ?? key);
    this._min = insertIntoList(this._min, node);
    if (key < this._min.key) this._min = node;
    this._nodes.set(value ?? key, node);
    this._size++;
    return node;
  }

  /** Peek at minimum. O(1). */
  peekMin() { return this._min ? { key: this._min.key, value: this._min.value } : undefined; }

  /** Extract minimum. O(log n) amortized. */
  extractMin() {
    if (!this._min) return undefined;
    const min = this._min;
    
    // Add children to root list
    if (min.child) {
      let child = min.child;
      do {
        const next = child.right;
        child.parent = null;
        this._min = insertIntoList(this._min, child);
        child = next;
      } while (child !== min.child);
    }
    
    // Remove min from root list
    this._min = removeFromList(min);
    
    if (this._min) {
      this._consolidate();
    }
    
    this._nodes.delete(min.value);
    this._size--;
    return { key: min.key, value: min.value };
  }

  /** Decrease key. O(1) amortized. */
  decreaseKey(value, newKey) {
    const node = this._nodes.get(value);
    if (!node || newKey >= node.key) return;
    
    node.key = newKey;
    const parent = node.parent;
    
    if (parent && node.key < parent.key) {
      this._cut(node, parent);
    }
    
    if (node.key < this._min.key) this._min = node;
  }

  _cut(node, parent) {
    parent.degree--;
    if (parent.child === node) {
      parent.child = node.right === node ? null : node.right;
    }
    removeFromList(node);
    node.parent = null;
    node.mark = false;
    this._min = insertIntoList(this._min, node);
  }

  _consolidate() {
    const maxDegree = Math.ceil(Math.log2(this._size + 1)) + 1;
    const A = new Array(maxDegree + 1).fill(null);
    
    // Collect all root nodes
    const roots = [];
    let current = this._min;
    do { roots.push(current); current = current.right; } while (current !== this._min);
    
    for (const root of roots) {
      let x = root;
      let d = x.degree;
      while (A[d]) {
        let y = A[d];
        if (x.key > y.key) [x, y] = [y, x];
        this._link(y, x);
        A[d] = null;
        d++;
      }
      A[d] = x;
    }
    
    // Rebuild root list and find new min
    this._min = null;
    for (const node of A) {
      if (node) {
        node.left = node;
        node.right = node;
        this._min = insertIntoList(this._min, node);
        if (!this._min || node.key < this._min.key) this._min = node;
      }
    }
  }

  _link(child, parent) {
    removeFromList(child);
    child.parent = parent;
    parent.child = insertIntoList(parent.child, child);
    parent.degree++;
    child.mark = false;
  }
}
