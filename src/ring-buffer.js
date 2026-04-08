// ring-buffer.js — Fixed-size circular buffer
// When full, new writes overwrite the oldest entries.
// O(1) push/pop, fixed memory footprint. Useful for:
// - Streaming data windows
// - Recent query history
// - Bounded event logs

export class RingBuffer {
  constructor(capacity) {
    this.capacity = capacity;
    this._buf = new Array(capacity);
    this._head = 0; // Next write position
    this._size = 0;
  }

  push(item) {
    this._buf[this._head] = item;
    this._head = (this._head + 1) % this.capacity;
    if (this._size < this.capacity) this._size++;
  }

  /** Get item at logical index (0 = oldest) */
  get(index) {
    if (index < 0 || index >= this._size) return undefined;
    const realIndex = (this._head - this._size + index + this.capacity) % this.capacity;
    return this._buf[realIndex];
  }

  /** Most recent item */
  peek() { return this._size > 0 ? this.get(this._size - 1) : undefined; }
  
  /** Oldest item */
  peekOldest() { return this._size > 0 ? this.get(0) : undefined; }

  /** Pop most recent item */
  pop() {
    if (this._size === 0) return undefined;
    this._head = (this._head - 1 + this.capacity) % this.capacity;
    this._size--;
    return this._buf[this._head];
  }

  get size() { return this._size; }
  get isFull() { return this._size === this.capacity; }
  get isEmpty() { return this._size === 0; }

  /** Iterate from oldest to newest */
  *[Symbol.iterator]() {
    for (let i = 0; i < this._size; i++) yield this.get(i);
  }

  toArray() { return [...this]; }

  clear() { this._head = 0; this._size = 0; }
}
