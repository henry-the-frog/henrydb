// skip-list.js — Skip List: probabilistic sorted data structure
// O(log n) search, insert, delete with O(n) space.
// Used in LevelDB/RocksDB memtables and Redis sorted sets.
// Concurrent-friendly: insertions only need local locks.

class SkipListNode {
  constructor(key, value, level) {
    this.key = key;
    this.value = value;
    this.forward = new Array(level + 1).fill(null); // forward[i] = next node at level i
  }
}

/**
 * SkipList — sorted key-value store with O(log n) operations.
 */
export class SkipList {
  constructor(maxLevel = 16, p = 0.5) {
    this.maxLevel = maxLevel;
    this.p = p;
    this.level = 0; // Current highest level
    this.header = new SkipListNode(null, null, maxLevel);
    this._size = 0;
    this.stats = { inserts: 0, searches: 0, comparisons: 0 };
  }

  /**
   * Insert or update a key-value pair.
   */
  set(key, value) {
    const update = new Array(this.maxLevel + 1).fill(null);
    let current = this.header;

    // Find position
    for (let i = this.level; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, key) < 0) {
        current = current.forward[i];
        this.stats.comparisons++;
      }
      update[i] = current;
    }

    current = current.forward[0];

    if (current !== null && this._compare(current.key, key) === 0) {
      // Update existing
      current.value = value;
      return;
    }

    // Insert new
    const newLevel = this._randomLevel();
    if (newLevel > this.level) {
      for (let i = this.level + 1; i <= newLevel; i++) {
        update[i] = this.header;
      }
      this.level = newLevel;
    }

    const newNode = new SkipListNode(key, value, newLevel);
    for (let i = 0; i <= newLevel; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;
    }

    this._size++;
    this.stats.inserts++;
  }

  /**
   * Search for a key. Returns value or undefined.
   */
  get(key) {
    this.stats.searches++;
    let current = this.header;

    for (let i = this.level; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, key) < 0) {
        current = current.forward[i];
        this.stats.comparisons++;
      }
    }

    current = current.forward[0];
    if (current !== null && this._compare(current.key, key) === 0) {
      return current.value;
    }
    return undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  /**
   * Delete a key. Returns true if found.
   */
  delete(key) {
    const update = new Array(this.maxLevel + 1).fill(null);
    let current = this.header;

    for (let i = this.level; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, key) < 0) {
        current = current.forward[i];
      }
      update[i] = current;
    }

    current = current.forward[0];
    if (current === null || this._compare(current.key, key) !== 0) return false;

    for (let i = 0; i <= this.level; i++) {
      if (update[i].forward[i] !== current) break;
      update[i].forward[i] = current.forward[i];
    }

    while (this.level > 0 && this.header.forward[this.level] === null) {
      this.level--;
    }

    this._size--;
    return true;
  }

  /**
   * Range scan: all entries with key in [lo, hi].
   */
  range(lo, hi) {
    const results = [];
    let current = this.header;

    for (let i = this.level; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, lo) < 0) {
        current = current.forward[i];
      }
    }

    current = current.forward[0];
    while (current !== null && this._compare(current.key, hi) <= 0) {
      results.push({ key: current.key, value: current.value });
      current = current.forward[0];
    }

    return results;
  }

  /**
   * Iterate all entries in sorted order.
   */
  *[Symbol.iterator]() {
    let current = this.header.forward[0];
    while (current !== null) {
      yield { key: current.key, value: current.value };
      current = current.forward[0];
    }
  }

  /**
   * First entry.
   */
  first() {
    const node = this.header.forward[0];
    return node ? { key: node.key, value: node.value } : null;
  }

  /**
   * Last entry (O(n) scan — skip lists don't have back pointers here).
   */
  last() {
    let current = this.header;
    for (let i = this.level; i >= 0; i--) {
      while (current.forward[i] !== null) current = current.forward[i];
    }
    return current !== this.header ? { key: current.key, value: current.value } : null;
  }

  get size() { return this._size; }

  _randomLevel() {
    let level = 0;
    while (Math.random() < this.p && level < this.maxLevel) level++;
    return level;
  }

  _compare(a, b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }

  getStats() {
    return {
      ...this.stats,
      size: this._size,
      currentLevel: this.level,
      avgComparisonsPerSearch: this.stats.searches > 0
        ? (this.stats.comparisons / this.stats.searches).toFixed(2)
        : '0',
    };
  }

  // Aliases for compatibility
  insert(key, value) { return this.set(key, value); }
  find(key) { return this.get(key); }
  search(key) { return this.get(key); }

  /**
   * Range query: return all values with keys in [low, high] inclusive.
   */
  range(low, high) {
    const results = [];
    let node = this.header;
    // Traverse to the first node >= low
    for (let level = this.maxLevel - 1; level >= 0; level--) {
      while (node.forward[level] && node.forward[level].key < low) {
        node = node.forward[level];
      }
    }
    node = node.forward[0];
    while (node && node.key <= high) {
      results.push({ key: node.key, value: node.value });
      node = node.forward[0];
    }
    return results;
  }
}
