// deque.js — Double-ended queue using circular buffer
// O(1) amortized push/pop from both front and back.

export class Deque {
  constructor(capacity = 16) {
    this._buf = new Array(capacity);
    this._head = 0;
    this._tail = 0;
    this._size = 0;
  }

  get size() { return this._size; }
  get isEmpty() { return this._size === 0; }

  pushBack(value) {
    if (this._size === this._buf.length) this._grow();
    this._buf[this._tail] = value;
    this._tail = (this._tail + 1) % this._buf.length;
    this._size++;
  }

  pushFront(value) {
    if (this._size === this._buf.length) this._grow();
    this._head = (this._head - 1 + this._buf.length) % this._buf.length;
    this._buf[this._head] = value;
    this._size++;
  }

  popBack() {
    if (this._size === 0) return undefined;
    this._tail = (this._tail - 1 + this._buf.length) % this._buf.length;
    const val = this._buf[this._tail];
    this._buf[this._tail] = undefined;
    this._size--;
    return val;
  }

  popFront() {
    if (this._size === 0) return undefined;
    const val = this._buf[this._head];
    this._buf[this._head] = undefined;
    this._head = (this._head + 1) % this._buf.length;
    this._size--;
    return val;
  }

  peekFront() { return this._size > 0 ? this._buf[this._head] : undefined; }
  peekBack() { return this._size > 0 ? this._buf[(this._tail - 1 + this._buf.length) % this._buf.length] : undefined; }

  at(index) {
    if (index < 0 || index >= this._size) return undefined;
    return this._buf[(this._head + index) % this._buf.length];
  }

  *[Symbol.iterator]() {
    for (let i = 0; i < this._size; i++) yield this._buf[(this._head + i) % this._buf.length];
  }

  toArray() { return [...this]; }

  _grow() {
    const newBuf = new Array(this._buf.length * 2);
    for (let i = 0; i < this._size; i++) {
      newBuf[i] = this._buf[(this._head + i) % this._buf.length];
    }
    this._head = 0;
    this._tail = this._size;
    this._buf = newBuf;
  }
}
