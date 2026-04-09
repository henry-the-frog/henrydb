// ring-buffer.js — Fixed-size circular buffer for HenryDB
//
// A ring buffer (circular buffer) provides O(1) push/pop from both ends
// using a fixed-size array. When full, new elements overwrite the oldest.
//
// Use cases in databases:
//   - WAL segment management
//   - Connection pool request queues
//   - Streaming aggregation windows
//   - Query result buffering
//   - Recent query log

/**
 * RingBuffer — Fixed-size circular buffer.
 */
export class RingBuffer {
  /**
   * @param {number} capacity - Maximum number of elements
   */
  constructor(capacity) {
    this.capacity = capacity;
    this._buffer = new Array(capacity);
    this._head = 0;    // Next write position
    this._tail = 0;    // Next read position
    this._size = 0;
    this._overflows = 0;
  }

  get size() { return this._size; }
  get isEmpty() { return this._size === 0; }
  get isFull() { return this._size === this.capacity; }

  /**
   * Push an element to the back. O(1).
   * If full, overwrites the oldest element.
   * Returns the overwritten element (or undefined).
   */
  push(value) {
    let overwritten = undefined;
    if (this._size === this.capacity) {
      overwritten = this._buffer[this._tail];
      this._tail = (this._tail + 1) % this.capacity;
      this._overflows++;
    } else {
      this._size++;
    }
    this._buffer[this._head] = value;
    this._head = (this._head + 1) % this.capacity;
    return overwritten;
  }

  /**
   * Pop the oldest element from the front. O(1).
   */
  shift() {
    if (this._size === 0) return undefined;
    const value = this._buffer[this._tail];
    this._buffer[this._tail] = undefined;
    this._tail = (this._tail + 1) % this.capacity;
    this._size--;
    return value;
  }

  /**
   * Peek at the oldest element without removing. O(1).
   */
  peekFront() {
    if (this._size === 0) return undefined;
    return this._buffer[this._tail];
  }

  /**
   * Peek at the newest element. O(1).
   */
  peekBack() {
    if (this._size === 0) return undefined;
    const idx = (this._head - 1 + this.capacity) % this.capacity;
    return this._buffer[idx];
  }

  /**
   * Get element at index (0 = oldest, size-1 = newest). O(1).
   */
  at(index) {
    if (index < 0 || index >= this._size) return undefined;
    return this._buffer[(this._tail + index) % this.capacity];
  }

  /**
   * Iterate all elements from oldest to newest.
   */
  *[Symbol.iterator]() {
    for (let i = 0; i < this._size; i++) {
      yield this._buffer[(this._tail + i) % this.capacity];
    }
  }

  /**
   * Convert to array (oldest first).
   */
  toArray() {
    return [...this];
  }

  /**
   * Clear all elements.
   */
  clear() {
    this._buffer.fill(undefined);
    this._head = 0;
    this._tail = 0;
    this._size = 0;
  }

  /**
   * Get statistics.
   */
  getStats() {
    return {
      size: this._size,
      capacity: this.capacity,
      overflows: this._overflows,
      fillRatio: this._size / this.capacity,
    };
  }
}
