// more-trees.js — Interval tree, order statistics tree, quadtree, splay tree, heap

/** Interval Tree — overlapping range queries */
export class IntervalTree {
  constructor() { this._intervals = []; }
  insert(lo, hi, data) { this._intervals.push({ lo, hi, data }); }
  query(point) { return this._intervals.filter(i => i.lo <= point && i.hi >= point).map(i => i.data); }
  overlap(lo, hi) { return this._intervals.filter(i => i.lo <= hi && i.hi >= lo).map(i => i.data); }
  get size() { return this._intervals.length; }
}

/** Order Statistics Tree — rank/select on augmented BST */
export class OrderStatisticsTree {
  constructor() { this.root = null; }
  insert(key) { this.root = this._insert(this.root, key); }
  _insert(node, key) {
    if (!node) return { key, left: null, right: null, size: 1 };
    if (key < node.key) node.left = this._insert(node.left, key);
    else if (key > node.key) node.right = this._insert(node.right, key);
    node.size = 1 + this._sz(node.left) + this._sz(node.right);
    return node;
  }
  /** Select the k-th smallest element (0-indexed) */
  select(k) {
    let node = this.root;
    while (node) {
      const leftSize = this._sz(node.left);
      if (k === leftSize) return node.key;
      if (k < leftSize) node = node.left;
      else { k -= leftSize + 1; node = node.right; }
    }
    return null;
  }
  /** Rank of key (number of keys less than key) */
  rank(key) {
    let r = 0, node = this.root;
    while (node) {
      if (key < node.key) node = node.left;
      else if (key > node.key) { r += this._sz(node.left) + 1; node = node.right; }
      else { r += this._sz(node.left); break; }
    }
    return r;
  }
  _sz(n) { return n ? n.size : 0; }
  get size() { return this._sz(this.root); }
}

/** Quadtree — spatial index for 2D points */
export class Quadtree {
  constructor(x, y, w, h, capacity = 4) {
    this.boundary = { x, y, w, h };
    this.capacity = capacity;
    this._points = [];
    this._divided = false;
    this.nw = this.ne = this.sw = this.se = null;
  }
  insert(point) {
    if (!this._contains(point)) return false;
    if (this._points.length < this.capacity) { this._points.push(point); return true; }
    if (!this._divided) this._subdivide();
    return this.nw.insert(point) || this.ne.insert(point) || this.sw.insert(point) || this.se.insert(point);
  }
  query(range) {
    const found = [];
    if (!this._intersects(range)) return found;
    for (const p of this._points) {
      if (p.x >= range.x && p.x <= range.x + range.w && p.y >= range.y && p.y <= range.y + range.h)
        found.push(p);
    }
    if (this._divided) {
      found.push(...this.nw.query(range), ...this.ne.query(range), ...this.sw.query(range), ...this.se.query(range));
    }
    return found;
  }
  _contains(p) { const b = this.boundary; return p.x >= b.x && p.x <= b.x + b.w && p.y >= b.y && p.y <= b.y + b.h; }
  _intersects(r) {
    const b = this.boundary;
    return !(r.x > b.x + b.w || r.x + r.w < b.x || r.y > b.y + b.h || r.y + r.h < b.y);
  }
  _subdivide() {
    const { x, y, w, h } = this.boundary;
    const hw = w / 2, hh = h / 2;
    this.nw = new Quadtree(x, y, hw, hh, this.capacity);
    this.ne = new Quadtree(x + hw, y, hw, hh, this.capacity);
    this.sw = new Quadtree(x, y + hh, hw, hh, this.capacity);
    this.se = new Quadtree(x + hw, y + hh, hw, hh, this.capacity);
    this._divided = true;
  }
}

/** BinaryHeap — min or max heap */
export class BinaryHeap {
  constructor(compare = (a, b) => a - b) { this._data = []; this._cmp = compare; }
  push(val) { this._data.push(val); this._siftUp(this._data.length - 1); }
  pop() {
    if (this._data.length === 0) return undefined;
    const top = this._data[0]; const last = this._data.pop();
    if (this._data.length > 0) { this._data[0] = last; this._siftDown(0); }
    return top;
  }
  peek() { return this._data[0]; }
  get size() { return this._data.length; }
  _siftUp(i) {
    while (i > 0) { const p = (i - 1) >> 1; if (this._cmp(this._data[i], this._data[p]) < 0) { [this._data[i], this._data[p]] = [this._data[p], this._data[i]]; i = p; } else break; }
  }
  _siftDown(i) {
    while (true) {
      let s = i; const l = 2*i+1, r = 2*i+2;
      if (l < this._data.length && this._cmp(this._data[l], this._data[s]) < 0) s = l;
      if (r < this._data.length && this._cmp(this._data[r], this._data[s]) < 0) s = r;
      if (s === i) break; [this._data[i], this._data[s]] = [this._data[s], this._data[i]]; i = s;
    }
  }
}

/** SplayTree — self-adjusting BST */
export class SplayTree {
  constructor() { this.root = null; this._size = 0; }
  _splay(node, key) {
    if (!node) return null;
    if (key < node.key) {
      if (!node.left) return node;
      if (key < node.left.key) { node.left.left = this._splay(node.left.left, key); node = this._rotR(node); }
      else if (key > node.left.key) { node.left.right = this._splay(node.left.right, key); if (node.left.right) node.left = this._rotL(node.left); }
      return node.left ? this._rotR(node) : node;
    } else if (key > node.key) {
      if (!node.right) return node;
      if (key > node.right.key) { node.right.right = this._splay(node.right.right, key); node = this._rotL(node); }
      else if (key < node.right.key) { node.right.left = this._splay(node.right.left, key); if (node.right.left) node.right = this._rotR(node.right); }
      return node.right ? this._rotL(node) : node;
    }
    return node;
  }
  insert(key, value) {
    if (!this.root) { this.root = { key, value, left: null, right: null }; this._size++; return; }
    this.root = this._splay(this.root, key);
    if (this.root.key === key) { this.root.value = value; return; }
    const n = { key, value, left: null, right: null };
    if (key < this.root.key) { n.left = this.root.left; n.right = this.root; this.root.left = null; }
    else { n.right = this.root.right; n.left = this.root; this.root.right = null; }
    this.root = n; this._size++;
  }
  get(key) { if (!this.root) return undefined; this.root = this._splay(this.root, key); return this.root.key === key ? this.root.value : undefined; }
  _rotR(n) { const l = n.left; n.left = l.right; l.right = n; return l; }
  _rotL(n) { const r = n.right; n.right = r.left; r.left = n; return r; }
  get size() { return this._size; }
}
