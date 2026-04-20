// bplus-tree.js — B+ Tree index
// Internal nodes: only keys + child pointers (no values)
// Leaf nodes: keys + values + next/prev pointers (doubly linked)
// Perfect for range scans: find start key, then follow leaf chain.

class BPlusNode {
  constructor(isLeaf = false, order = 4) {
    this.isLeaf = isLeaf;
    this.order = order;
    this.keys = [];
    this.children = []; // For internal: child nodes. For leaf: values.
    this.next = null; // Leaf → next leaf
    this.prev = null; // Leaf → prev leaf
  }

  get isFull() { return this.keys.length >= this.order - 1; }
}

/**
 * BPlusTree — B+ tree index with range scan support.
 */
export class BPlusTree {
  constructor(order = 4) {
    if (order < 3) throw new Error('Order must be ≥ 3');
    this.order = order;
    this.root = new BPlusNode(true, order);
    this._size = 0;
  }

  /**
   * Insert a key-value pair.
   */
  insert(key, value) {
    const result = this._insertRec(this.root, key, value);
    if (result) {
      // Root was split — create new root
      const newRoot = new BPlusNode(false, this.order);
      newRoot.keys = [result.key];
      newRoot.children = [this.root, result.node];
      this.root = newRoot;
    }
    this._size++;
  }

  _insertRec(node, key, value) {
    if (node.isLeaf) {
      // Insert into leaf
      const pos = this._findPos(node.keys, key);
      
      // Check for duplicate
      if (pos < node.keys.length && node.keys[pos] === key) {
        node.children[pos] = value; // Update
        this._size--; // Undo increment
        return null;
      }

      node.keys.splice(pos, 0, key);
      node.children.splice(pos, 0, value);

      if (node.keys.length >= this.order) {
        return this._splitLeaf(node);
      }
      return null;
    }

    // Internal node: find child
    const pos = this._findPos(node.keys, key);
    const result = this._insertRec(node.children[pos], key, value);

    if (result) {
      // Child was split — insert separator key
      node.keys.splice(pos, 0, result.key);
      node.children.splice(pos + 1, 0, result.node);

      if (node.keys.length >= this.order) {
        return this._splitInternal(node);
      }
    }
    return null;
  }

  _splitLeaf(node) {
    const mid = Math.ceil(node.keys.length / 2);
    const newLeaf = new BPlusNode(true, this.order);
    
    newLeaf.keys = node.keys.splice(mid);
    newLeaf.children = node.children.splice(mid);
    
    // Maintain linked list
    newLeaf.next = node.next;
    newLeaf.prev = node;
    if (node.next) node.next.prev = newLeaf;
    node.next = newLeaf;

    return { key: newLeaf.keys[0], node: newLeaf };
  }

  _splitInternal(node) {
    const mid = Math.floor(node.keys.length / 2);
    const separatorKey = node.keys[mid];
    const newNode = new BPlusNode(false, this.order);
    
    // Keys after separator go to new node
    newNode.keys = node.keys.slice(mid + 1);
    // Children after separator go to new node
    newNode.children = node.children.slice(mid + 1);
    
    // Keep only keys before separator in old node
    node.keys = node.keys.slice(0, mid);
    // Keep only children up to and including mid
    node.children = node.children.slice(0, mid + 1);

    return { key: separatorKey, node: newNode };
  }

  /**
   * Search for a key. Returns value or undefined.
   */
  get(key) {
    let node = this.root;
    while (!node.isLeaf) {
      const pos = this._findChildPos(node.keys, key);
      node = node.children[pos];
    }
    const pos = node.keys.indexOf(key);
    return pos >= 0 ? node.children[pos] : undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  /** Alias for get() — compatibility with BTree API */
  search(key) { return this.get(key); }

  /**
   * Delete a key (simplified: doesn't rebalance/merge nodes).
   */
  delete(key) {
    let node = this.root;
    while (!node.isLeaf) {
      const pos = this._findChildPos(node.keys, key);
      node = node.children[pos];
    }
    const pos = node.keys.indexOf(key);
    if (pos < 0) return false;
    node.keys.splice(pos, 1);
    node.children.splice(pos, 1);
    this._size--;
    return true;
  }

  /**
   * Range scan: all entries with key in [lo, hi].
   * Follows leaf linked list for O(k) range queries.
   */
  range(lo, hi) {
    const results = [];
    // Find leaf containing lo
    let node = this.root;
    while (!node.isLeaf) {
      const pos = this._findChildPos(node.keys, lo);
      node = node.children[pos];
    }

    // Scan leaves
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        if (node.keys[i] > hi) return results;
        if (node.keys[i] >= lo) {
          results.push({ key: node.keys[i], value: node.children[i] });
        }
      }
      node = node.next;
    }
    return results;
  }

  /**
   * Find minimum key.
   */
  min() {
    let node = this.root;
    while (!node.isLeaf) node = node.children[0];
    return node.keys.length > 0 ? { key: node.keys[0], value: node.children[0] } : null;
  }

  /**
   * Find maximum key.
   */
  max() {
    let node = this.root;
    while (!node.isLeaf) node = node.children[node.children.length - 1];
    const last = node.keys.length - 1;
    return last >= 0 ? { key: node.keys[last], value: node.children[last] } : null;
  }

  /**
   * Iterate all entries in sorted order via leaf chain.
   */
  *[Symbol.iterator]() {
    let node = this.root;
    while (!node.isLeaf) node = node.children[0];
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        yield { key: node.keys[i], value: node.children[i] };
      }
      node = node.next;
    }
  }

  get size() { return this._size; }

  _findPos(keys, key) {
    let lo = 0, hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (keys[mid] < key) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /**
   * Find the child index in an internal node.
   * If key <= keys[i], go to children[i]. If key > all keys, go to last child.
   */
  _findChildPos(keys, key) {
    let i = 0;
    while (i < keys.length && key >= keys[i]) i++;
    return i;
  }

  get height() {
    let h = 0, node = this.root;
    while (!node.isLeaf) { h++; node = node.children[0]; }
    return h + 1;
  }
}
