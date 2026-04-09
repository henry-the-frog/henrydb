// minmax-heap.js — Double-ended priority queue
// Supports O(1) findMin + findMax and O(log n) extractMin + extractMax.
// Even levels are min levels, odd levels are max levels.

export class MinMaxHeap {
  constructor() { this._data = []; }
  get size() { return this._data.length; }
  get isEmpty() { return this._data.length === 0; }

  peekMin() { return this._data[0]; }

  peekMax() {
    if (this._data.length <= 1) return this._data[0];
    if (this._data.length === 2) return this._data[1];
    return this._data[1] > this._data[2] ? this._data[1] : this._data[2];
  }

  push(value) {
    this._data.push(value);
    this._pushUp(this._data.length - 1);
  }

  popMin() {
    if (this._data.length === 0) return undefined;
    const min = this._data[0];
    const last = this._data.pop();
    if (this._data.length > 0) {
      this._data[0] = last;
      this._pushDown(0);
    }
    return min;
  }

  popMax() {
    if (this._data.length <= 1) return this._data.pop();
    if (this._data.length === 2) return this._data.pop();
    
    const maxIdx = this._data[1] >= this._data[2] ? 1 : 2;
    const max = this._data[maxIdx];
    const last = this._data.pop();
    if (maxIdx < this._data.length) {
      this._data[maxIdx] = last;
      this._pushDown(maxIdx);
    }
    return max;
  }

  _isMinLevel(i) { return Math.floor(Math.log2(i + 1)) % 2 === 0; }
  _parent(i) { return Math.floor((i - 1) / 2); }

  _pushUp(i) {
    if (i === 0) return;
    const p = this._parent(i);
    if (this._isMinLevel(i)) {
      if (this._data[i] > this._data[p]) {
        this._swap(i, p);
        this._pushUpMax(p);
      } else {
        this._pushUpMin(i);
      }
    } else {
      if (this._data[i] < this._data[p]) {
        this._swap(i, p);
        this._pushUpMin(p);
      } else {
        this._pushUpMax(i);
      }
    }
  }

  _pushUpMin(i) {
    const gp = this._parent(this._parent(i));
    if (i > 2 && this._data[i] < this._data[gp]) {
      this._swap(i, gp);
      this._pushUpMin(gp);
    }
  }

  _pushUpMax(i) {
    const gp = this._parent(this._parent(i));
    if (i > 2 && this._data[i] > this._data[gp]) {
      this._swap(i, gp);
      this._pushUpMax(gp);
    }
  }

  _pushDown(i) {
    if (this._isMinLevel(i)) this._pushDownMin(i);
    else this._pushDownMax(i);
  }

  _pushDownMin(i) {
    const m = this._smallestChildOrGrandchild(i);
    if (m === -1) return;
    
    if (this._isGrandchild(i, m)) {
      if (this._data[m] < this._data[i]) {
        this._swap(m, i);
        const p = this._parent(m);
        if (this._data[m] > this._data[p]) this._swap(m, p);
        this._pushDownMin(m);
      }
    } else {
      if (this._data[m] < this._data[i]) this._swap(m, i);
    }
  }

  _pushDownMax(i) {
    const m = this._largestChildOrGrandchild(i);
    if (m === -1) return;
    
    if (this._isGrandchild(i, m)) {
      if (this._data[m] > this._data[i]) {
        this._swap(m, i);
        const p = this._parent(m);
        if (this._data[m] < this._data[p]) this._swap(m, p);
        this._pushDownMax(m);
      }
    } else {
      if (this._data[m] > this._data[i]) this._swap(m, i);
    }
  }

  _smallestChildOrGrandchild(i) {
    const candidates = this._childrenAndGrandchildren(i);
    if (candidates.length === 0) return -1;
    return candidates.reduce((best, c) => this._data[c] < this._data[best] ? c : best);
  }

  _largestChildOrGrandchild(i) {
    const candidates = this._childrenAndGrandchildren(i);
    if (candidates.length === 0) return -1;
    return candidates.reduce((best, c) => this._data[c] > this._data[best] ? c : best);
  }

  _childrenAndGrandchildren(i) {
    const result = [];
    const l = 2 * i + 1, r = 2 * i + 2;
    if (l < this._data.length) {
      result.push(l);
      const ll = 2 * l + 1, lr = 2 * l + 2;
      if (ll < this._data.length) result.push(ll);
      if (lr < this._data.length) result.push(lr);
    }
    if (r < this._data.length) {
      result.push(r);
      const rl = 2 * r + 1, rr = 2 * r + 2;
      if (rl < this._data.length) result.push(rl);
      if (rr < this._data.length) result.push(rr);
    }
    return result;
  }

  _isGrandchild(i, m) {
    const l = 2 * i + 1, r = 2 * i + 2;
    return m !== l && m !== r;
  }

  _swap(i, j) { [this._data[i], this._data[j]] = [this._data[j], this._data[i]]; }
}
