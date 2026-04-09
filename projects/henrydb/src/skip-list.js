// skip-list.js — Skip list data structure for HenryDB
//
// A skip list is a probabilistic alternative to balanced trees.
// It uses multiple levels of linked lists with "express lanes" that skip
// over intermediate elements, providing O(log n) search, insert, and delete.
//
// Advantages over B+trees:
//   - Simpler implementation (no rotations or splits)
//   - Naturally concurrent (lock-free variants possible)
//   - Good cache behavior for in-memory workloads
//   - Easy range iteration
//
// Used by: Redis (sorted sets), LevelDB/RocksDB (memtable), MemSQL

const MAX_LEVEL = 32;
const P = 0.25; // Probability of promotion to next level (1/4 like Redis)

function randomLevel() {
  let level = 1;
  while (Math.random() < P && level < MAX_LEVEL) level++;
  return level;
}

class SkipNode {
  constructor(key, value, level) {
    this.key = key;
    this.value = value;
    this.forward = new Array(level).fill(null);
  }
}

/**
 * SkipList — Ordered key-value store with O(log n) operations.
 */
export class SkipList {
  constructor(comparator) {
    this._compare = comparator || ((a, b) => {
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    });
    
    this._header = new SkipNode(null, null, MAX_LEVEL);
    this._level = 1;
    this._size = 0;
  }

  get size() { return this._size; }
  get height() { return this._level; }

  /**
   * Insert a key-value pair. O(log n) expected.
   * If key exists, updates the value.
   */
  insert(key, value) {
    const update = new Array(MAX_LEVEL).fill(null);
    let current = this._header;

    // Find position (from top level down)
    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, key) < 0) {
        current = current.forward[i];
      }
      update[i] = current;
    }

    current = current.forward[0];

    // Key already exists — update value
    if (current !== null && this._compare(current.key, key) === 0) {
      current.value = value;
      return false; // Updated, not new
    }

    // Insert new node with random level
    const newLevel = randomLevel();
    if (newLevel > this._level) {
      for (let i = this._level; i < newLevel; i++) {
        update[i] = this._header;
      }
      this._level = newLevel;
    }

    const newNode = new SkipNode(key, value, newLevel);
    for (let i = 0; i < newLevel; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;
    }

    this._size++;
    return true; // New insertion
  }

  /**
   * Search for a key. O(log n) expected.
   * Returns the value, or undefined if not found.
   */
  get(key) {
    let current = this._header;
    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, key) < 0) {
        current = current.forward[i];
      }
    }
    current = current.forward[0];
    if (current !== null && this._compare(current.key, key) === 0) {
      return current.value;
    }
    return undefined;
  }

  /**
   * Delete a key. O(log n) expected.
   * Returns true if the key was found and removed.
   */
  delete(key) {
    const update = new Array(MAX_LEVEL).fill(null);
    let current = this._header;

    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, key) < 0) {
        current = current.forward[i];
      }
      update[i] = current;
    }

    current = current.forward[0];

    if (current === null || this._compare(current.key, key) !== 0) {
      return false; // Not found
    }

    // Remove from all levels
    for (let i = 0; i < this._level; i++) {
      if (update[i].forward[i] !== current) break;
      update[i].forward[i] = current.forward[i];
    }

    // Reduce level if needed
    while (this._level > 1 && this._header.forward[this._level - 1] === null) {
      this._level--;
    }

    this._size--;
    return true;
  }

  /**
   * Iterate all entries in key order.
   */
  *[Symbol.iterator]() {
    let current = this._header.forward[0];
    while (current !== null) {
      yield { key: current.key, value: current.value };
      current = current.forward[0];
    }
  }

  /**
   * Range query: all entries with key in [low, high] inclusive.
   */
  *range(low, high) {
    let current = this._header;
    
    // Find first node >= low
    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] !== null && this._compare(current.forward[i].key, low) < 0) {
        current = current.forward[i];
      }
    }
    
    current = current.forward[0];
    
    // Yield all nodes in range
    while (current !== null && this._compare(current.key, high) <= 0) {
      yield { key: current.key, value: current.value };
      current = current.forward[0];
    }
  }

  /**
   * Get the minimum key.
   */
  min() {
    const first = this._header.forward[0];
    return first ? { key: first.key, value: first.value } : null;
  }

  /**
   * Get the maximum key.
   */
  max() {
    let current = this._header;
    for (let i = this._level - 1; i >= 0; i--) {
      while (current.forward[i] !== null) {
        current = current.forward[i];
      }
    }
    return current !== this._header ? { key: current.key, value: current.value } : null;
  }

  /**
   * Check if a key exists.
   */
  has(key) {
    return this.get(key) !== undefined;
  }

  /**
   * Get statistics about the skip list.
   */
  getStats() {
    // Count nodes at each level
    const levelCounts = new Array(this._level).fill(0);
    for (let i = 0; i < this._level; i++) {
      let current = this._header.forward[i];
      while (current !== null) {
        levelCounts[i]++;
        current = current.forward[i];
      }
    }
    
    return {
      size: this._size,
      height: this._level,
      levelCounts,
      memoryEstimate: this._size * 64 + levelCounts.reduce((s, c) => s + c * 8, 0), // rough estimate
    };
  }
}
