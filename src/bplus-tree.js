// bplus-tree.js — B+ Tree for HenryDB
// Leaf nodes store key-value pairs and are linked for range scans.

/**
 * B+ Tree: balanced tree with data only in leaf nodes.
 * Supports point lookups, range scans, and ordered iteration.
 */
export class BPlusTree {
  constructor(order = 4) {
    this._order = order; // Max keys per node
    this._root = { keys: [], values: [], leaf: true, next: null };
    this._size = 0;
  }

  /**
   * Insert a key-value pair.
   */
  insert(key, value) {
    const result = this._insertRecursive(this._root, key, value);
    if (result) {
      // Root was split, create new root
      const newRoot = {
        keys: [result.splitKey],
        children: [this._root, result.newNode],
        leaf: false,
      };
      this._root = newRoot;
    }
    this._size++;
  }

  /**
   * Find value by key.
   */
  find(key) {
    let node = this._root;
    while (!node.leaf) {
      let i = 0;
      while (i < node.keys.length && key >= node.keys[i]) i++;
      node = node.children[i];
    }
    const idx = node.keys.indexOf(key);
    return idx >= 0 ? node.values[idx] : undefined;
  }

  /**
   * Range scan: return all key-value pairs where minKey <= key <= maxKey.
   */
  range(minKey, maxKey) {
    // Find the leaf containing minKey
    let node = this._root;
    while (!node.leaf) {
      let i = 0;
      while (i < node.keys.length && minKey >= node.keys[i]) i++;
      node = node.children[i];
    }
    
    const result = [];
    // Scan through linked leaf nodes
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        if (node.keys[i] >= minKey && node.keys[i] <= maxKey) {
          result.push({ key: node.keys[i], value: node.values[i] });
        }
        if (node.keys[i] > maxKey) return result;
      }
      node = node.next;
    }
    return result;
  }

  /**
   * Iterate all entries in order.
   */
  entries() {
    const result = [];
    let node = this._root;
    while (!node.leaf) node = node.children[0];
    
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        result.push({ key: node.keys[i], value: node.values[i] });
      }
      node = node.next;
    }
    return result;
  }

  /**
   * Delete a key.
   */
  delete(key) {
    const deleted = this._deleteFromLeaf(key);
    if (deleted) this._size--;
    return deleted;
  }

  get size() { return this._size; }

  _insertRecursive(node, key, value) {
    if (node.leaf) {
      // Find insertion point
      let i = 0;
      while (i < node.keys.length && node.keys[i] < key) i++;
      
      // Update if key exists
      if (i < node.keys.length && node.keys[i] === key) {
        node.values[i] = value;
        this._size--; // Will be re-incremented
        return null;
      }
      
      node.keys.splice(i, 0, key);
      node.values.splice(i, 0, value);
      
      if (node.keys.length > this._order) {
        return this._splitLeaf(node);
      }
      return null;
    }
    
    // Internal node
    let i = 0;
    while (i < node.keys.length && key >= node.keys[i]) i++;
    
    const result = this._insertRecursive(node.children[i], key, value);
    if (result) {
      node.keys.splice(i, 0, result.splitKey);
      node.children.splice(i + 1, 0, result.newNode);
      
      if (node.keys.length > this._order) {
        return this._splitInternal(node);
      }
    }
    return null;
  }

  _splitLeaf(node) {
    const mid = Math.floor(node.keys.length / 2);
    const newNode = {
      keys: node.keys.splice(mid),
      values: node.values.splice(mid),
      leaf: true,
      next: node.next,
    };
    node.next = newNode;
    return { splitKey: newNode.keys[0], newNode };
  }

  _splitInternal(node) {
    const mid = Math.floor(node.keys.length / 2);
    const splitKey = node.keys[mid];
    const newNode = {
      keys: node.keys.splice(mid + 1),
      children: node.children.splice(mid + 1),
      leaf: false,
    };
    node.keys.splice(mid); // Remove the promoted key
    return { splitKey, newNode };
  }

  _deleteFromLeaf(key) {
    let node = this._root;
    while (!node.leaf) {
      let i = 0;
      while (i < node.keys.length && key >= node.keys[i]) i++;
      node = node.children[i];
    }
    const idx = node.keys.indexOf(key);
    if (idx < 0) return false;
    node.keys.splice(idx, 1);
    node.values.splice(idx, 1);
    return true;
  }
}
