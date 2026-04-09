// concurrent-queue.js — Thread-safe queue simulation with backpressure
export class BoundedQueue {
  constructor(capacity) {
    this._buf = [];
    this._capacity = capacity;
  }

  enqueue(item) {
    if (this._buf.length >= this._capacity) return false; // Backpressure
    this._buf.push(item);
    return true;
  }

  dequeue() { return this._buf.shift(); }
  get size() { return this._buf.length; }
  get isEmpty() { return this._buf.length === 0; }
  get isFull() { return this._buf.length >= this._capacity; }
}
