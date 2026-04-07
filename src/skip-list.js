// skip-list.js — Skip list index for HenryDB
// Probabilistic data structure with O(log n) expected search/insert/delete.
// Alternative to B+tree that's simpler to implement and lock-free friendly.

class SkipListNode {
  constructor(key, value, level) {
    this.key = key;
    this.value = value;
    this.forward = new Array(level + 1).fill(null); // Forward pointers for each level
  }
}

export class SkipList {
  constructor(maxLevel = 16, probability = 0.5) {
    this._maxLevel = maxLevel;
    this._probability = probability;
    this._level = 0;
    this._size = 0;
    this._header = new SkipListNode(null, null, maxLevel);
  }

  /**
   * Generate random level for a new node.
   * Each level has `probability` chance of being promoted.
   */
  _randomLevel() {
    let level = 0;
    while (Math.random() < this._probability && level < this._maxLevel) {
      level++;
    }
    return level;
  }

  /**
   * Insert a key-value pair. O(log n) expected.
   */
  insert(key, value) {
    const update = new Array(this._maxLevel + 1).fill(null);
    let current = this._header;

    // Find insertion point at each level
    for (let i = this._level; i >= 0; i--) {
      while (current.forward[i] !== null && current.forward[i].key < key) {
        current = current.forward[i];
      }
      update[i] = current;
    }

    const newLevel = this._randomLevel();
    
    // If new level is higher than current max, update header pointers
    if (newLevel > this._level) {
      for (let i = this._level + 1; i <= newLevel; i++) {
        update[i] = this._header;
      }
      this._level = newLevel;
    }

    const newNode = new SkipListNode(key, value, newLevel);
    
    // Insert node by updating forward pointers
    for (let i = 0; i <= newLevel; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;
    }

    this._size++;
  }

  /**
   * Search for a key. Returns the value or null. O(log n) expected.
   */
  find(key) {
    let current = this._header;
    
    for (let i = this._level; i >= 0; i--) {
      while (current.forward[i] !== null && current.forward[i].key < key) {
        current = current.forward[i];
      }
    }
    
    current = current.forward[0];
    if (current !== null && current.key === key) {
      return current.value;
    }
    return null;
  }

  /**
   * Find all values with a given key.
   */
  findAll(key) {
    const results = [];
    let current = this._header;
    
    for (let i = this._level; i >= 0; i--) {
      while (current.forward[i] !== null && current.forward[i].key < key) {
        current = current.forward[i];
      }
    }
    
    current = current.forward[0];
    while (current !== null && current.key === key) {
      results.push(current.value);
      current = current.forward[0];
    }
    return results;
  }

  /**
   * Range scan: return all entries where low <= key <= high.
   */
  range(low, high) {
    const results = [];
    let current = this._header;
    
    // Navigate to first node >= low
    for (let i = this._level; i >= 0; i--) {
      while (current.forward[i] !== null && current.forward[i].key < low) {
        current = current.forward[i];
      }
    }
    
    current = current.forward[0];
    while (current !== null && current.key <= high) {
      results.push({ key: current.key, value: current.value });
      current = current.forward[0];
    }
    return results;
  }

  /**
   * Delete a key. O(log n) expected.
   */
  delete(key) {
    const update = new Array(this._maxLevel + 1).fill(null);
    let current = this._header;

    for (let i = this._level; i >= 0; i--) {
      while (current.forward[i] !== null && current.forward[i].key < key) {
        current = current.forward[i];
      }
      update[i] = current;
    }

    current = current.forward[0];
    if (current !== null && current.key === key) {
      for (let i = 0; i <= this._level; i++) {
        if (update[i].forward[i] !== current) break;
        update[i].forward[i] = current.forward[i];
      }
      
      // Reduce level if needed
      while (this._level > 0 && this._header.forward[this._level] === null) {
        this._level--;
      }
      this._size--;
      return true;
    }
    return false;
  }

  get size() { return this._size; }

  /**
   * Iterate all entries in sorted order.
   */
  *[Symbol.iterator]() {
    let current = this._header.forward[0];
    while (current !== null) {
      yield { key: current.key, value: current.value };
      current = current.forward[0];
    }
  }

  /**
   * Get statistics about the skip list structure.
   */
  stats() {
    const levelCounts = new Array(this._level + 1).fill(0);
    let current = this._header.forward[0];
    while (current !== null) {
      for (let i = 0; i < current.forward.length; i++) {
        if (i <= this._level) levelCounts[i]++;
      }
      current = current.forward[0];
    }
    return {
      size: this._size,
      maxLevel: this._level,
      levelCounts,
    };
  }
}
