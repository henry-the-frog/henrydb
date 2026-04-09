// topk.js — Maintain top-k elements from a stream using a min-heap
// O(n log k) total, O(1) space beyond k elements.

import { MinHeap } from './priority-queue.js';

export class TopK {
  constructor(k) {
    this._k = k;
    this._heap = new MinHeap((a, b) => a.score - b.score);
  }

  get size() { return this._heap.size; }

  add(item, score) {
    if (this._heap.size < this._k) {
      this._heap.push({ item, score });
    } else if (score > this._heap.peek().score) {
      this._heap.pop();
      this._heap.push({ item, score });
    }
  }

  /** Get top-k sorted by score descending. */
  getTop() {
    return [...this._heap].sort((a, b) => b.score - a.score).map(e => e.item);
  }

  /** Get minimum score in the top-k. */
  threshold() { return this._heap.isEmpty ? -Infinity : this._heap.peek().score; }
}
