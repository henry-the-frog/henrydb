// lfu-cache.js — O(1) Least Frequently Used Cache
// Each operation (get, put) is O(1) using a frequency-to-DLL map.
// Used in: database buffer pools, CDN caches, CPU caches.

class DLLNode {
  constructor(key, value) {
    this.key = key;
    this.value = value;
    this.freq = 1;
    this.prev = null;
    this.next = null;
  }
}

class DoublyLinkedList {
  constructor() {
    this.head = new DLLNode(null, null);
    this.tail = new DLLNode(null, null);
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this.size = 0;
  }

  addFirst(node) {
    node.next = this.head.next;
    node.prev = this.head;
    this.head.next.prev = node;
    this.head.next = node;
    this.size++;
  }

  remove(node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
    this.size--;
  }

  removeLast() {
    if (this.size === 0) return null;
    const node = this.tail.prev;
    this.remove(node);
    return node;
  }

  isEmpty() { return this.size === 0; }
}

export class LFUCache {
  /**
   * @param {number} capacity - Maximum number of entries
   */
  constructor(capacity) {
    this._capacity = capacity;
    this._size = 0;
    this._minFreq = 0;
    this._keyMap = new Map();      // key → DLLNode
    this._freqMap = new Map();     // freq → DoublyLinkedList
    this._hits = 0;
    this._misses = 0;
  }

  get size() { return this._size; }
  get capacity() { return this._capacity; }

  /**
   * Get value by key. O(1).
   * Updates frequency.
   */
  get(key) {
    const node = this._keyMap.get(key);
    if (!node) { this._misses++; return undefined; }
    this._hits++;
    this._updateFreq(node);
    return node.value;
  }

  /**
   * Put key-value pair. O(1).
   * Evicts LFU entry if at capacity.
   */
  put(key, value) {
    if (this._capacity <= 0) return;
    
    const existing = this._keyMap.get(key);
    if (existing) {
      existing.value = value;
      this._updateFreq(existing);
      return;
    }
    
    // Evict if full
    if (this._size >= this._capacity) {
      const list = this._freqMap.get(this._minFreq);
      const evicted = list.removeLast();
      this._keyMap.delete(evicted.key);
      this._size--;
    }
    
    // Insert new
    const node = new DLLNode(key, value);
    this._keyMap.set(key, node);
    if (!this._freqMap.has(1)) this._freqMap.set(1, new DoublyLinkedList());
    this._freqMap.get(1).addFirst(node);
    this._minFreq = 1;
    this._size++;
  }

  _updateFreq(node) {
    const oldFreq = node.freq;
    const oldList = this._freqMap.get(oldFreq);
    oldList.remove(node);
    
    if (oldFreq === this._minFreq && oldList.isEmpty()) {
      this._minFreq++;
    }
    
    node.freq++;
    if (!this._freqMap.has(node.freq)) this._freqMap.set(node.freq, new DoublyLinkedList());
    this._freqMap.get(node.freq).addFirst(node);
  }

  getStats() {
    return {
      size: this._size,
      capacity: this._capacity,
      hitRate: this._hits / (this._hits + this._misses) || 0,
      hits: this._hits,
      misses: this._misses,
      minFreq: this._minFreq,
    };
  }
}
