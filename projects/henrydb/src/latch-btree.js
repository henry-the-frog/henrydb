// latch-btree.js — B+Tree with Latch Crabbing for Concurrent Access
//
// Latch crabbing (also called lock coupling) protocol:
// - Readers: acquire read latch on child, release parent read latch
// - Writers: acquire write latch on child, if child is "safe" (won't split/merge),
//           release ALL ancestor write latches. Otherwise keep them.
//
// "Safe" means:
//   - For inserts: node is not full (won't need to split)
//   - For deletes: node is more than half full (won't need to merge)
//
// This gives high concurrency: multiple readers can proceed in parallel,
// and writers only hold latches on the path that might be modified.

/**
 * Latch — simple read/write lock for simulation.
 * In a real system, these would be lightweight spinlocks or mutexes.
 */
export class Latch {
  constructor() {
    this.readers = 0;
    this.writer = false;
    this._waitQueue = [];
    this.name = '';
  }

  acquireRead() {
    if (this.writer) return false;
    this.readers++;
    return true;
  }

  acquireWrite() {
    if (this.writer || this.readers > 0) return false;
    this.writer = true;
    return true;
  }

  releaseRead() {
    if (this.readers > 0) this.readers--;
  }

  releaseWrite() {
    this.writer = false;
  }

  isHeld() {
    return this.readers > 0 || this.writer;
  }
}

/**
 * B+Tree node with latch.
 */
class BNode {
  constructor(order, isLeaf = false) {
    this.order = order;
    this.isLeaf = isLeaf;
    this.keys = [];
    this.values = [];        // Only for leaf nodes
    this.children = [];      // Only for internal nodes
    this.next = null;        // Sibling pointer (leaf only)
    this.latch = new Latch();
    this.id = BNode._nextId++;
    this.latch.name = `node-${this.id}`;
  }

  get isFull() { return this.keys.length >= this.order - 1; }
  get isMinimal() { return this.keys.length <= Math.ceil(this.order / 2) - 1; }
  get isSafeForInsert() { return !this.isFull; }
  get isSafeForDelete() { return !this.isMinimal; }
}
BNode._nextId = 0;

/**
 * LatchBPlusTree — B+Tree with latch crabbing protocol.
 */
export class LatchBPlusTree {
  constructor(order = 4) {
    this.order = order;
    this.root = new BNode(order, true);
    this._size = 0;
    this.stats = { latchAcquires: 0, latchReleases: 0, splits: 0, searches: 0 };
  }

  get size() { return this._size; }

  // ============================================================
  // Search (Read path with latch crabbing)
  // ============================================================

  /**
   * Search for a key. Returns the value or undefined.
   * Latch protocol: acquire read latch on child, release parent.
   */
  search(key) {
    this.stats.searches++;
    const heldLatches = [];
    
    // Acquire read latch on root
    this.root.latch.acquireRead();
    heldLatches.push(this.root);
    this.stats.latchAcquires++;
    
    let node = this.root;
    
    while (!node.isLeaf) {
      const childIdx = this._findChildIndex(node, key);
      const child = node.children[childIdx];
      
      // Acquire read latch on child
      child.latch.acquireRead();
      this.stats.latchAcquires++;
      
      // Release parent read latch (crab forward)
      node.latch.releaseRead();
      this.stats.latchReleases++;
      heldLatches.pop();
      
      heldLatches.push(child);
      node = child;
    }
    
    // Now at leaf — search for key
    const idx = node.keys.indexOf(key);
    const result = idx >= 0 ? node.values[idx] : undefined;
    
    // Release leaf latch
    node.latch.releaseRead();
    this.stats.latchReleases++;
    
    return result;
  }

  // ============================================================
  // Insert (Write path with latch crabbing)
  // ============================================================

  /**
   * Insert a key-value pair.
   * Latch protocol: hold write latches until we find a safe node,
   * then release all ancestor latches.
   */
  insert(key, value) {
    const heldLatches = []; // Stack of nodes with write latches
    
    // Acquire write latch on root
    this.root.latch.acquireWrite();
    heldLatches.push(this.root);
    this.stats.latchAcquires++;
    
    let node = this.root;
    
    while (!node.isLeaf) {
      const childIdx = this._findChildIndex(node, key);
      const child = node.children[childIdx];
      
      // Acquire write latch on child
      child.latch.acquireWrite();
      heldLatches.push(child);
      this.stats.latchAcquires++;
      
      // If child is safe (won't split), release ALL ancestor latches
      if (child.isSafeForInsert) {
        while (heldLatches.length > 1) {
          const released = heldLatches.shift();
          released.latch.releaseWrite();
          this.stats.latchReleases++;
        }
      }
      
      node = child;
    }
    
    // Now at leaf with write latch — insert
    this._insertIntoLeaf(node, key, value);
    this._size++;
    
    // If leaf overflowed, split upward
    if (node.keys.length >= this.order) {
      this._splitLeaf(node, heldLatches);
    }
    
    // Release all remaining latches
    for (const n of heldLatches) {
      n.latch.releaseWrite();
      this.stats.latchReleases++;
    }
  }

  // ============================================================
  // Range scan (Read path, leaf traversal)
  // ============================================================

  /**
   * Scan all key-value pairs in order.
   */
  *scan(startKey = null, endKey = null) {
    // Find the leaf node
    let node = this.root;
    node.latch.acquireRead();
    
    while (!node.isLeaf) {
      const childIdx = startKey !== null ? this._findChildIndex(node, startKey) : 0;
      const child = node.children[childIdx];
      child.latch.acquireRead();
      node.latch.releaseRead();
      node = child;
    }
    
    // Traverse leaves using sibling pointers
    while (node) {
      for (let i = 0; i < node.keys.length; i++) {
        if (startKey !== null && node.keys[i] < startKey) continue;
        if (endKey !== null && node.keys[i] > endKey) {
          node.latch.releaseRead();
          return;
        }
        yield { key: node.keys[i], value: node.values[i] };
      }
      
      const next = node.next;
      if (next) next.latch.acquireRead();
      node.latch.releaseRead();
      node = next;
    }
  }

  // ============================================================
  // Internal helpers
  // ============================================================

  _findChildIndex(node, key) {
    let i = 0;
    while (i < node.keys.length && key >= node.keys[i]) i++;
    return i;
  }

  _insertIntoLeaf(node, key, value) {
    let i = 0;
    while (i < node.keys.length && node.keys[i] < key) i++;
    if (i < node.keys.length && node.keys[i] === key) {
      node.values[i] = value; // Update existing
      this._size--; // Don't double-count
      return;
    }
    node.keys.splice(i, 0, key);
    node.values.splice(i, 0, value);
  }

  _splitLeaf(leaf, heldLatches) {
    const mid = Math.ceil(leaf.keys.length / 2);
    const newLeaf = new BNode(this.order, true);
    
    newLeaf.keys = leaf.keys.splice(mid);
    newLeaf.values = leaf.values.splice(mid);
    newLeaf.next = leaf.next;
    leaf.next = newLeaf;
    
    const promoteKey = newLeaf.keys[0];
    this.stats.splits++;
    
    // Find parent in held latches
    const leafIdx = heldLatches.indexOf(leaf);
    if (leafIdx === 0) {
      // Leaf is root — create new root
      const newRoot = new BNode(this.order, false);
      newRoot.keys = [promoteKey];
      newRoot.children = [leaf, newLeaf];
      this.root = newRoot;
    } else {
      const parent = heldLatches[leafIdx - 1];
      this._insertIntoInternal(parent, promoteKey, newLeaf, heldLatches.slice(0, leafIdx));
    }
  }

  _insertIntoInternal(node, key, rightChild, ancestors) {
    let i = 0;
    while (i < node.keys.length && node.keys[i] < key) i++;
    node.keys.splice(i, 0, key);
    node.children.splice(i + 1, 0, rightChild);
    
    // If internal node overflowed, split it too
    if (node.keys.length >= this.order) {
      this._splitInternal(node, ancestors);
    }
  }

  _splitInternal(node, ancestors) {
    const mid = Math.floor(node.keys.length / 2);
    const promoteKey = node.keys[mid];
    
    const newNode = new BNode(this.order, false);
    newNode.keys = node.keys.splice(mid + 1);
    newNode.children = node.children.splice(mid + 1);
    node.keys.splice(mid); // Remove promoted key
    
    this.stats.splits++;
    
    const nodeIdx = ancestors.indexOf(node);
    if (nodeIdx <= 0) {
      // Node is root — create new root
      const newRoot = new BNode(this.order, false);
      newRoot.keys = [promoteKey];
      newRoot.children = [node, newNode];
      this.root = newRoot;
    } else {
      const parent = ancestors[nodeIdx - 1];
      this._insertIntoInternal(parent, promoteKey, newNode, ancestors.slice(0, nodeIdx));
    }
  }

  /**
   * Get tree statistics.
   */
  getStats() {
    let height = 0;
    let node = this.root;
    while (node) {
      height++;
      node = node.isLeaf ? null : node.children[0];
    }
    
    let leafCount = 0;
    let internalCount = 0;
    const countNodes = (n) => {
      if (n.isLeaf) leafCount++;
      else {
        internalCount++;
        for (const c of n.children) countNodes(c);
      }
    };
    countNodes(this.root);
    
    return {
      size: this._size,
      height,
      leafNodes: leafCount,
      internalNodes: internalCount,
      ...this.stats,
    };
  }
}
