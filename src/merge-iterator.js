// merge-iterator.js — K-way merge iterator with min-heap priority queue
// Merges K sorted iterables into one sorted output.
// O(n log k) total, O(k) space. Used for:
// - External sort merge phase
// - LSM-tree compaction
// - Parallel sorted stream merging

class MinHeap {
  constructor(compare) {
    this._data = [];
    this._compare = compare;
  }

  push(item) {
    this._data.push(item);
    this._siftUp(this._data.length - 1);
  }

  pop() {
    if (this._data.length === 0) return undefined;
    const top = this._data[0];
    const last = this._data.pop();
    if (this._data.length > 0) {
      this._data[0] = last;
      this._siftDown(0);
    }
    return top;
  }

  peek() { return this._data[0]; }
  get size() { return this._data.length; }

  _siftUp(i) {
    while (i > 0) {
      const parent = (i - 1) >>> 1;
      if (this._compare(this._data[i], this._data[parent]) < 0) {
        [this._data[i], this._data[parent]] = [this._data[parent], this._data[i]];
        i = parent;
      } else break;
    }
  }

  _siftDown(i) {
    const n = this._data.length;
    while (true) {
      let smallest = i;
      const left = 2 * i + 1, right = 2 * i + 2;
      if (left < n && this._compare(this._data[left], this._data[smallest]) < 0) smallest = left;
      if (right < n && this._compare(this._data[right], this._data[smallest]) < 0) smallest = right;
      if (smallest === i) break;
      [this._data[i], this._data[smallest]] = [this._data[smallest], this._data[i]];
      i = smallest;
    }
  }
}

/**
 * MergeIterator — k-way merge of sorted iterables.
 */
export class MergeIterator {
  constructor(iterables, compare = (a, b) => a < b ? -1 : a > b ? 1 : 0) {
    this._compare = compare;
    this._iterators = iterables.map(it => it[Symbol.iterator]());
    this._heap = new MinHeap((a, b) => compare(a.value, b.value));
    
    // Prime the heap with one element from each iterator
    for (let i = 0; i < this._iterators.length; i++) {
      const result = this._iterators[i].next();
      if (!result.done) {
        this._heap.push({ value: result.value, iterIdx: i });
      }
    }
  }

  *[Symbol.iterator]() {
    while (this._heap.size > 0) {
      const { value, iterIdx } = this._heap.pop();
      yield value;
      
      const next = this._iterators[iterIdx].next();
      if (!next.done) {
        this._heap.push({ value: next.value, iterIdx });
      }
    }
  }

  toArray() { return [...this]; }
}

/**
 * Merge K sorted arrays into one sorted array.
 */
export function kWayMerge(arrays, compare) {
  return new MergeIterator(arrays, compare).toArray();
}

/**
 * MergeIterator with deduplication — keep only the latest (last) value for each key.
 */
export class DeduplicatingMergeIterator {
  constructor(iterables, keyFn = v => v, compare = (a, b) => a < b ? -1 : a > b ? 1 : 0) {
    this._inner = new MergeIterator(iterables, compare);
    this._keyFn = keyFn;
  }

  *[Symbol.iterator]() {
    let prev = undefined;
    let prevKey = undefined;
    
    for (const value of this._inner) {
      const key = this._keyFn(value);
      if (prevKey !== undefined && key === prevKey) {
        prev = value; // Keep latest
        continue;
      }
      if (prev !== undefined) yield prev;
      prev = value;
      prevKey = key;
    }
    if (prev !== undefined) yield prev;
  }

  toArray() { return [...this]; }
}

export { MinHeap };
