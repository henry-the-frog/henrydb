// b-epsilon-tree.js — Write-optimized B-tree with message buffers
// Used in TokuDB, BetrFS. Key insight: buffer writes at internal nodes,
// flush down when buffer is full. This turns random writes into sequential I/O.
//
// O(log_B(N) / B^ε) amortized write cost (vs O(log_B(N)) for B-tree)
// where ε controls the write/read tradeoff (0 = B-tree, 1 = max write opt)

class BENode {
  constructor(isLeaf = false) {
    this.isLeaf = isLeaf;
    this.keys = [];
    this.children = []; // For internal nodes
    this.values = [];   // For leaf nodes
    this.buffer = [];   // Message buffer (internal nodes only)
  }
}

export class BEpsilonTree {
  /**
   * @param {number} B - Node fanout (branching factor)
   * @param {number} bufferSize - Messages per node before flush
   */
  constructor(B = 16, bufferSize = 32) {
    this._B = B;
    this._bufferSize = bufferSize;
    this._root = new BENode(true);
    this._size = 0;
    this._flushCount = 0;
  }

  get size() { return this._size; }
  get flushCount() { return this._flushCount; }

  /**
   * Insert (or upsert). O(1) amortized to buffer.
   */
  put(key, value) {
    this._addMessage(this._root, { type: 'PUT', key, value });
    this._size++; // Approximate; deletes not tracked precisely
  }

  /**
   * Delete a key. Buffers a DELETE message.
   */
  delete(key) {
    this._addMessage(this._root, { type: 'DELETE', key });
  }

  /**
   * Get: must flush path to leaf to get current value.
   */
  get(key) {
    return this._get(this._root, key);
  }

  /**
   * Scan all entries in sorted order.
   */
  scan() {
    const results = [];
    this._scan(this._root, results);
    return results.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  _addMessage(node, msg) {
    if (node.isLeaf) {
      // Apply directly to leaf
      this._applyToLeaf(node, msg);
      if (node.keys.length > this._B) {
        this._splitLeaf(node);
      }
      return;
    }

    node.buffer.push(msg);
    if (node.buffer.length >= this._bufferSize) {
      this._flushBuffer(node);
    }
  }

  _flushBuffer(node) {
    this._flushCount++;
    const msgs = node.buffer;
    node.buffer = [];

    // Route each message to appropriate child
    for (const msg of msgs) {
      const childIdx = this._findChild(node, msg.key);
      const child = node.children[childIdx];
      this._addMessage(child, msg);
    }
  }

  _applyToLeaf(leaf, msg) {
    const idx = this._bsearch(leaf.keys, msg.key);
    
    if (msg.type === 'PUT') {
      if (idx < leaf.keys.length && leaf.keys[idx] === msg.key) {
        leaf.values[idx] = msg.value; // Update
      } else {
        leaf.keys.splice(idx, 0, msg.key);
        leaf.values.splice(idx, 0, msg.value);
      }
    } else if (msg.type === 'DELETE') {
      if (idx < leaf.keys.length && leaf.keys[idx] === msg.key) {
        leaf.keys.splice(idx, 1);
        leaf.values.splice(idx, 1);
      }
    }
  }

  _splitLeaf(leaf) {
    // Simple: promote to parent (or create root)
    const mid = Math.floor(leaf.keys.length / 2);
    const right = new BENode(true);
    right.keys = leaf.keys.splice(mid);
    right.values = leaf.values.splice(mid);

    if (leaf === this._root) {
      const newRoot = new BENode(false);
      newRoot.keys = [right.keys[0]];
      newRoot.children = [leaf, right];
      this._root = newRoot;
    }
    // Note: full implementation would propagate splits up
  }

  _get(node, key) {
    if (node.isLeaf) {
      const idx = this._bsearch(node.keys, key);
      if (idx < node.keys.length && node.keys[idx] === key) return node.values[idx];
      return undefined;
    }

    // Check buffer first (most recent message wins)
    for (let i = node.buffer.length - 1; i >= 0; i--) {
      if (node.buffer[i].key === key) {
        if (node.buffer[i].type === 'DELETE') return undefined;
        return node.buffer[i].value;
      }
    }

    const childIdx = this._findChild(node, key);
    return this._get(node.children[childIdx], key);
  }

  _scan(node, results) {
    if (node.isLeaf) {
      for (let i = 0; i < node.keys.length; i++) {
        results.push({ key: node.keys[i], value: node.values[i] });
      }
      return;
    }

    // Flush buffer before scan to ensure consistency
    if (node.buffer.length > 0) {
      this._flushBuffer(node);
    }
    for (const child of node.children) {
      this._scan(child, results);
    }
  }

  _findChild(node, key) {
    let i = 0;
    while (i < node.keys.length && key >= node.keys[i]) i++;
    return Math.min(i, node.children.length - 1);
  }

  _bsearch(arr, key) {
    let lo = 0, hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (arr[mid] < key) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  getStats() {
    return {
      size: this._size,
      flushes: this._flushCount,
      bufferSize: this._bufferSize,
      fanout: this._B,
    };
  }
}
