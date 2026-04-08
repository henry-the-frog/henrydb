// cow-btree.js — Copy-on-Write B-tree for snapshot isolation
// Every write creates new nodes instead of mutating existing ones.
// Old versions remain accessible through saved root pointers (snapshots).
// Used in LMDB, Btrfs, and append-only databases.

class COWNode {
  constructor(isLeaf, keys = [], values = [], children = []) {
    this.isLeaf = isLeaf;
    this.keys = keys;
    this.values = values; // Only for leaves
    this.children = children; // Only for internals
  }

  clone() {
    return new COWNode(
      this.isLeaf,
      [...this.keys],
      [...this.values],
      [...this.children],
    );
  }
}

export class COWBTree {
  constructor(order = 4) {
    this.order = order;
    this._root = new COWNode(true);
    this._snapshots = new Map(); // snapshotId → root
    this._nextSnapshotId = 0;
    this._size = 0;
  }

  /**
   * Create a snapshot (save current root).
   */
  snapshot() {
    const id = this._nextSnapshotId++;
    this._snapshots.set(id, this._root);
    return id;
  }

  /**
   * Read from a snapshot.
   */
  getFromSnapshot(snapshotId, key) {
    const root = this._snapshots.get(snapshotId);
    if (!root) return undefined;
    return this._get(root, key);
  }

  /**
   * Insert (creates new path from root to leaf).
   */
  set(key, value) {
    const { root, grew } = this._insertCOW(this._root, key, value);
    this._root = root;
    if (grew) this._size++;
  }

  get(key) { return this._get(this._root, key); }
  has(key) { return this.get(key) !== undefined; }

  _get(node, key) {
    if (node.isLeaf) {
      const idx = node.keys.indexOf(key);
      return idx >= 0 ? node.values[idx] : undefined;
    }
    let i = 0;
    while (i < node.keys.length && key >= node.keys[i]) i++;
    return this._get(node.children[i], key);
  }

  _insertCOW(node, key, value) {
    const newNode = node.clone(); // Copy-on-write

    if (newNode.isLeaf) {
      const idx = newNode.keys.indexOf(key);
      if (idx >= 0) {
        newNode.values[idx] = value;
        return { root: newNode, grew: false };
      }
      // Insert sorted
      let pos = 0;
      while (pos < newNode.keys.length && newNode.keys[pos] < key) pos++;
      newNode.keys.splice(pos, 0, key);
      newNode.values.splice(pos, 0, value);

      if (newNode.keys.length >= this.order) {
        return { root: this._splitLeafCOW(newNode), grew: true };
      }
      return { root: newNode, grew: true };
    }

    // Internal node
    let i = 0;
    while (i < newNode.keys.length && key >= newNode.keys[i]) i++;
    const { root: childRoot, grew } = this._insertCOW(newNode.children[i], key, value);
    
    if (childRoot.isLeaf === false && childRoot.keys.length === 1 && childRoot.children.length === 2 && newNode.children[i] !== childRoot) {
      // Child was split — need to absorb separator
      // Actually, check if childRoot IS a split result (new internal with 2 children)
    }
    
    newNode.children[i] = childRoot;

    if (childRoot.keys && childRoot.keys.length >= this.order) {
      // Need to split child and pull up separator
      const mid = Math.floor(childRoot.keys.length / 2);
      if (childRoot.isLeaf) {
        const left = new COWNode(true, childRoot.keys.slice(0, mid), childRoot.values.slice(0, mid));
        const right = new COWNode(true, childRoot.keys.slice(mid), childRoot.values.slice(mid));
        const sep = right.keys[0];
        newNode.children[i] = left;
        newNode.keys.splice(i, 0, sep);
        newNode.children.splice(i + 1, 0, right);
      } else {
        const sep = childRoot.keys[mid];
        const left = new COWNode(false, childRoot.keys.slice(0, mid), [], childRoot.children.slice(0, mid + 1));
        const right = new COWNode(false, childRoot.keys.slice(mid + 1), [], childRoot.children.slice(mid + 1));
        newNode.children[i] = left;
        newNode.keys.splice(i, 0, sep);
        newNode.children.splice(i + 1, 0, right);
      }
    }

    if (newNode.keys.length >= this.order) {
      return { root: this._splitInternalCOW(newNode), grew };
    }

    return { root: newNode, grew };
  }

  _splitLeafCOW(node) {
    const mid = Math.floor(node.keys.length / 2);
    const left = new COWNode(true, node.keys.slice(0, mid), node.values.slice(0, mid));
    const right = new COWNode(true, node.keys.slice(mid), node.values.slice(mid));
    const newRoot = new COWNode(false, [right.keys[0]], [], [left, right]);
    return newRoot;
  }

  _splitInternalCOW(node) {
    const mid = Math.floor(node.keys.length / 2);
    const sep = node.keys[mid];
    const left = new COWNode(false, node.keys.slice(0, mid), [], node.children.slice(0, mid + 1));
    const right = new COWNode(false, node.keys.slice(mid + 1), [], node.children.slice(mid + 1));
    return new COWNode(false, [sep], [], [left, right]);
  }

  delete(key) {
    // Simplified: mark as deleted by setting value to undefined
    const existing = this.get(key);
    if (existing === undefined) return false;
    this.set(key, undefined);
    this._size--;
    return true;
  }

  get size() { return this._size; }
  get snapshotCount() { return this._snapshots.size; }
}
