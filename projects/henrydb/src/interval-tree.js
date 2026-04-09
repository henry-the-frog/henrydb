// interval-tree.js — Augmented BST for interval queries
// Find all intervals that overlap with a query point or range.
// Used for: temporal queries, scheduling, geospatial ranges.

class ITNode {
  constructor(low, high, value) {
    this.low = low;
    this.high = high;
    this.value = value;
    this.max = high; // Max high in subtree
    this.left = null;
    this.right = null;
  }
}

export class IntervalTree {
  constructor() { this._root = null; this._size = 0; }
  get size() { return this._size; }

  insert(low, high, value) {
    this._root = this._insert(this._root, low, high, value);
    this._size++;
  }

  _insert(node, low, high, value) {
    if (!node) return new ITNode(low, high, value);
    if (low < node.low) node.left = this._insert(node.left, low, high, value);
    else node.right = this._insert(node.right, low, high, value);
    node.max = Math.max(node.max, high);
    return node;
  }

  /** Find all intervals containing point. */
  queryPoint(point) {
    const results = [];
    this._queryPoint(this._root, point, results);
    return results;
  }

  _queryPoint(node, point, results) {
    if (!node) return;
    if (node.low <= point && point <= node.high) {
      results.push({ low: node.low, high: node.high, value: node.value });
    }
    if (node.left && node.left.max >= point) {
      this._queryPoint(node.left, point, results);
    }
    this._queryPoint(node.right, point, results);
  }

  /** Find all intervals overlapping [qLow, qHigh]. */
  queryRange(qLow, qHigh) {
    const results = [];
    this._queryRange(this._root, qLow, qHigh, results);
    return results;
  }

  _queryRange(node, qLow, qHigh, results) {
    if (!node) return;
    if (node.low <= qHigh && qLow <= node.high) {
      results.push({ low: node.low, high: node.high, value: node.value });
    }
    if (node.left && node.left.max >= qLow) {
      this._queryRange(node.left, qLow, qHigh, results);
    }
    if (node.low <= qHigh) {
      this._queryRange(node.right, qLow, qHigh, results);
    }
  }
}
