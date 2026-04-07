// btree.js — B+ Tree index for HenryDB
// In-memory B+ tree with configurable order, supporting insert, search, range scan, delete

const DEFAULT_ORDER = 32; // max keys per node

// Default comparator for single values
function defaultComparator(a, b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

// Composite key comparator: compare arrays element by element
export function compositeComparator(a, b) {
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    if (a[i] < b[i]) return -1;
    if (a[i] > b[i]) return 1;
  }
  return a.length - b.length;
}

// Suffix comparator for non-unique indexes: [userKey, suffix]
// Ensures deterministic ordering even with duplicate user keys
function suffixComparator(a, b) {
  // a and b are [userKey, suffix] arrays
  const ka = a[0], kb = b[0];
  if (ka < kb) return -1;
  if (ka > kb) return 1;
  // Same user key — compare suffix
  const sa = a[1], sb = b[1];
  if (sa < sb) return -1;
  if (sa > sb) return 1;
  return 0;
}

export class BPlusTree {
  constructor(order = DEFAULT_ORDER, { unique = true, comparator = null } = {}) {
    this.order = order;
    this.unique = unique;
    this._nextSuffix = 0;
    
    // For non-unique trees, use a suffix comparator internally
    if (!unique && !comparator) {
      this.comparator = suffixComparator;
      this._useSuffix = true;
    } else {
      this.comparator = comparator || defaultComparator;
      this._useSuffix = false;
    }
    this.root = new LeafNode(order, unique, this.comparator);
  }

  // Search for exact key
  search(key) {
    if (this._useSuffix) {
      // For non-unique trees, search returns the first matching entry
      const results = this._rangeByUserKey(key, key);
      return results.length > 0 ? results[0].value : undefined;
    }
    let node = this.root;
    while (node instanceof InternalNode) {
      node = node.findChild(key);
    }
    return node.search(key);
  }

  // Insert key-value pair
  insert(key, value) {
    let internalKey = key;
    if (this._useSuffix) {
      // Non-unique: wrap key with suffix for uniqueness
      internalKey = [key, this._nextSuffix++];
    }
    const result = this._insertInto(this.root, internalKey, value);
    if (result) {
      // Root was split — create new root
      const newRoot = new InternalNode(this.order);
      newRoot.keys = [result.key];
      newRoot.children = [this.root, result.node];
      this.root = newRoot;
    }
  }

  _insertInto(node, key, value) {
    if (node instanceof LeafNode) {
      node.insert(key, value);
      if (node.keys.length >= this.order) return node.split();
      return null;
    }

    // Internal node
    const child = node.findChild(key, this.comparator);
    const result = this._insertInto(child, key, value);
    if (!result) return null;

    // Child was split — insert separator
    node.insertKey(result.key, result.node, this.comparator);
    if (node.keys.length >= this.order) return node.split();
    return null;
  }

  // Delete a key (for suffix trees, deletes the first entry matching this user key)
  delete(key, value) {
    if (this._useSuffix) {
      return this._deleteByUserKey(key, value);
    }
    return this._deleteFrom(this.root, key);
  }

  // Delete by scanning for user key match in suffix tree
  _deleteByUserKey(userKey, targetValue) {
    // Find the leaf containing entries with this user key
    const lo = [userKey, -Infinity];
    let node = this.root;
    while (node instanceof InternalNode) {
      node = node.findChild(lo, this.comparator);
    }
    // Scan leaves to find the matching entry
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        const k = node.keys[i];
        if (k[0] === userKey) {
          // Match by value if provided, otherwise first match
          if (targetValue === undefined || node.values[i] === targetValue) {
            node.keys.splice(i, 1);
            node.values.splice(i, 1);
            return true;
          }
        }
        if (k[0] > userKey) return false; // Past the range
      }
      node = node.next;
    }
    return false;
  }

  _deleteFrom(node, key) {
    if (node instanceof LeafNode) {
      return node.delete(key);
    }
    const child = node.findChild(key, this.comparator);
    return this._deleteFrom(child, key);
  }

  // Range scan: all values where lo <= key <= hi
  range(lo, hi) {
    if (this._useSuffix) {
      return this._rangeByUserKey(lo, hi);
    }
    const results = [];
    // Start from leftmost leaf and scan all — duplicate key splits can create
    // out-of-order leaf chains, so we can't early-terminate on first key > hi.
    let node = this.root;
    while (node instanceof InternalNode) node = node.children[0];
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        if (node.keys[i] >= lo && node.keys[i] <= hi) {
          results.push({ key: node.keys[i], value: node.values[i] });
        }
      }
      node = node.next;
    }
    return results;
  }

  // Range scan for suffix trees: translate user keys to internal composite keys
  _rangeByUserKey(lo, hi) {
    const results = [];
    const loKey = [lo, -Infinity];
    
    // Navigate to the starting leaf
    let node = this.root;
    while (node instanceof InternalNode) {
      node = node.findChild(loKey, this.comparator);
    }
    
    // Scan through leaves with early termination (safe because suffix keys maintain order)
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        const k = node.keys[i];
        if (k[0] > hi) return results; // Early termination — safe with suffix keys!
        if (k[0] >= lo && k[0] <= hi) {
          results.push({ key: k[0], value: node.values[i] });
        }
      }
      node = node.next;
    }
    return results;
  }

  // Scan all entries in order
  *scan() {
    let node = this.root;
    while (node instanceof InternalNode) node = node.children[0];
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        const key = this._useSuffix ? node.keys[i][0] : node.keys[i];
        yield { key, value: node.values[i] };
      }
      node = node.next;
    }
  }

  // Bulk load sorted data
  static bulkLoad(entries, order = DEFAULT_ORDER) {
    const tree = new BPlusTree(order);
    for (const { key, value } of entries) {
      tree.insert(key, value);
    }
    return tree;
  }

  // Count total entries
  get size() {
    let count = 0;
    for (const _ of this.scan()) count++;
    return count;
  }

  // Tree height
  get height() {
    let h = 1;
    let node = this.root;
    while (node instanceof InternalNode) { h++; node = node.children[0]; }
    return h;
  }

  // Min key
  min() {
    let node = this.root;
    while (node instanceof InternalNode) node = node.children[0];
    if (node.keys.length === 0) return null;
    const key = this._useSuffix ? node.keys[0][0] : node.keys[0];
    return { key, value: node.values[0] };
  }

  // Max key
  max() {
    let node = this.root;
    while (node instanceof InternalNode) node = node.children[node.children.length - 1];
    if (node.keys.length === 0) return null;
    const lastKey = node.keys[node.keys.length - 1];
    const key = this._useSuffix ? lastKey[0] : lastKey;
    return { key, value: node.values[node.values.length - 1] };
  }
}

// ===== Leaf Node =====
class LeafNode {
  constructor(order, unique = true, comparator = null) {
    this.order = order;
    this.unique = unique;
    this.comparator = comparator || defaultComparator;
    this.keys = [];
    this.values = [];
    this.next = null; // linked list pointer
  }

  search(key) {
    const idx = this._findIndex(key);
    if (idx < this.keys.length && this.comparator(this.keys[idx], key) === 0) return this.values[idx];
    return undefined;
  }

  insert(key, value) {
    const idx = this._findIndex(key);
    if (this.unique && idx < this.keys.length && this.comparator(this.keys[idx], key) === 0) {
      this.values[idx] = value; // update for unique indexes
      return;
    }
    this.keys.splice(idx, 0, key);
    this.values.splice(idx, 0, value);
  }

  delete(key) {
    const idx = this._findIndex(key);
    if (idx < this.keys.length && this.comparator(this.keys[idx], key) === 0) {
      this.keys.splice(idx, 1);
      this.values.splice(idx, 1);
      return true;
    }
    return false;
  }

  split() {
    const mid = Math.ceil(this.keys.length / 2);
    const newLeaf = new LeafNode(this.order, this.unique, this.comparator);
    newLeaf.keys = this.keys.splice(mid);
    newLeaf.values = this.values.splice(mid);
    newLeaf.next = this.next;
    this.next = newLeaf;
    return { key: newLeaf.keys[0], node: newLeaf };
  }

  _findIndex(key) {
    let lo = 0, hi = this.keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.comparator(this.keys[mid], key) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
}

// ===== Internal Node =====
class InternalNode {
  constructor(order) {
    this.order = order;
    this.keys = [];     // separator keys
    this.children = []; // children.length = keys.length + 1
  }

  findChild(key, comparator) {
    const cmp = comparator || defaultComparator;
    let i = 0;
    while (i < this.keys.length && cmp(key, this.keys[i]) >= 0) i++;
    return this.children[i];
  }

  // For range scan: navigate to the leftmost child that could contain key
  // Uses strict > so we go left when key == separator (duplicates span children)
  findChildForScan(key, comparator) {
    const cmp = comparator || defaultComparator;
    let i = 0;
    while (i < this.keys.length && cmp(key, this.keys[i]) > 0) i++;
    return this.children[i];
  }

  insertKey(key, rightChild, comparator) {
    const cmp = comparator || defaultComparator;
    let i = 0;
    while (i < this.keys.length && cmp(key, this.keys[i]) > 0) i++;
    this.keys.splice(i, 0, key);
    this.children.splice(i + 1, 0, rightChild);
  }

  split() {
    const mid = Math.floor(this.keys.length / 2);
    const pushUpKey = this.keys[mid];
    const newInternal = new InternalNode(this.order);
    newInternal.keys = this.keys.splice(mid + 1);
    newInternal.children = this.children.splice(mid + 1);
    this.keys.pop(); // remove the pushed-up key
    return { key: pushUpKey, node: newInternal };
  }
}
