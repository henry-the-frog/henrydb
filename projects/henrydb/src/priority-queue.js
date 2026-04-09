// priority-queue.js — Binary heap priority queue
// O(log n) insert and extractMin. Used for k-way merges, top-k, scheduling.

export class MinHeap {
  constructor(comparator) {
    this._compare = comparator || ((a, b) => a - b);
    this._data = [];
  }

  get size() { return this._data.length; }
  get isEmpty() { return this._data.length === 0; }

  peek() { return this._data[0]; }

  push(value) {
    this._data.push(value);
    this._bubbleUp(this._data.length - 1);
  }

  pop() {
    if (this._data.length === 0) return undefined;
    const top = this._data[0];
    const last = this._data.pop();
    if (this._data.length > 0) {
      this._data[0] = last;
      this._sinkDown(0);
    }
    return top;
  }

  _bubbleUp(i) {
    while (i > 0) {
      const parent = (i - 1) >>> 1;
      if (this._compare(this._data[i], this._data[parent]) >= 0) break;
      [this._data[i], this._data[parent]] = [this._data[parent], this._data[i]];
      i = parent;
    }
  }

  _sinkDown(i) {
    const n = this._data.length;
    while (true) {
      let smallest = i;
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      if (left < n && this._compare(this._data[left], this._data[smallest]) < 0) smallest = left;
      if (right < n && this._compare(this._data[right], this._data[smallest]) < 0) smallest = right;
      if (smallest === i) break;
      [this._data[i], this._data[smallest]] = [this._data[smallest], this._data[i]];
      i = smallest;
    }
  }

  *[Symbol.iterator]() {
    const copy = [...this._data];
    const heap = new MinHeap(this._compare);
    heap._data = copy;
    while (!heap.isEmpty) yield heap.pop();
  }
}
