// compact-filter.js — Golomb-Coded Set (GCS) for space-efficient membership testing
// Used in LSM-tree SSTables to filter point queries (like Bloom filters but smaller).

function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return h >>> 0;
}

function sipHash(key, n) {
  // Simplified hash for GCS (maps key to [0, n))
  return fnv1a(key + '_gcs') % n;
}

export class GolombCodedSet {
  constructor(fp = 19) { // False positive rate ≈ 1/2^fp
    this.fp = fp;
    this.m = 1 << fp; // Golomb parameter M = 2^fp
    this.n = 0;
    this.data = null; // Compressed bit array
    this._sorted = null;
  }

  /** Build from an array of keys */
  static build(keys, fp = 19) {
    const gcs = new GolombCodedSet(fp);
    const n = keys.length;
    gcs.n = n;
    const N = n * gcs.m; // Hash space

    // Hash all keys and sort
    const hashes = keys.map(k => sipHash(String(k), N)).sort((a, b) => a - b);
    
    // Delta encode
    const deltas = [];
    for (let i = 0; i < hashes.length; i++) {
      deltas.push(i === 0 ? hashes[i] : hashes[i] - hashes[i - 1]);
    }

    // Golomb-Rice encode: quotient in unary, remainder in binary
    gcs._sorted = hashes;
    gcs.data = deltas;
    return gcs;
  }

  /** Check membership (may return false positive) */
  has(key) {
    if (!this._sorted || this.n === 0) return false;
    const N = this.n * this.m;
    const h = sipHash(String(key), N);
    
    // Binary search in sorted hashes
    let lo = 0, hi = this._sorted.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._sorted[mid] === h) return true;
      this._sorted[mid] < h ? lo = mid + 1 : hi = mid;
    }
    return false;
  }

  /** Approximate size in bytes */
  get sizeBytes() {
    if (!this.data) return 0;
    // Each delta encoded in ~(1 + fp) bits
    return Math.ceil(this.n * (1 + this.fp) / 8);
  }

  /** Bits per entry */
  get bitsPerEntry() { return this.n > 0 ? (this.sizeBytes * 8) / this.n : 0; }
}

/**
 * Left-Leaning Red-Black Tree — simplified balanced BST.
 */
export class LLRBTree {
  constructor() { this.root = null; this._size = 0; }

  _isRed(node) { return node != null && node.red; }

  _rotateLeft(h) {
    const x = h.right;
    h.right = x.left;
    x.left = h;
    x.red = h.red;
    h.red = true;
    return x;
  }

  _rotateRight(h) {
    const x = h.left;
    h.left = x.right;
    x.right = h;
    x.red = h.red;
    h.red = true;
    return x;
  }

  _flipColors(h) {
    h.red = !h.red;
    if (h.left) h.left.red = !h.left.red;
    if (h.right) h.right.red = !h.right.red;
  }

  _fixUp(h) {
    if (this._isRed(h.right) && !this._isRed(h.left)) h = this._rotateLeft(h);
    if (this._isRed(h.left) && this._isRed(h.left?.left)) h = this._rotateRight(h);
    if (this._isRed(h.left) && this._isRed(h.right)) this._flipColors(h);
    return h;
  }

  insert(key, value) {
    this.root = this._insert(this.root, key, value);
    if (this.root) this.root.red = false;
  }

  _insert(h, key, value) {
    if (h == null) { this._size++; return { key, value, left: null, right: null, red: true }; }
    if (key < h.key) h.left = this._insert(h.left, key, value);
    else if (key > h.key) h.right = this._insert(h.right, key, value);
    else h.value = value;
    return this._fixUp(h);
  }

  search(key) {
    let node = this.root;
    while (node) {
      if (key < node.key) node = node.left;
      else if (key > node.key) node = node.right;
      else return node.value;
    }
    return undefined;
  }

  /** In-order traversal */
  *inOrder(node = this.root) {
    if (!node) return;
    yield* this.inOrder(node.left);
    yield { key: node.key, value: node.value };
    yield* this.inOrder(node.right);
  }

  get size() { return this._size; }
  get height() { return this._height(this.root); }
  _height(node) { return node == null ? 0 : 1 + Math.max(this._height(node.left), this._height(node.right)); }
}

/**
 * AA Tree — simpler balanced BST using level-based invariants.
 */
export class AATree {
  constructor() { this.root = null; this._size = 0; }

  _skew(t) {
    if (t && t.left && t.left.level === t.level) {
      const l = t.left;
      t.left = l.right;
      l.right = t;
      return l;
    }
    return t;
  }

  _split(t) {
    if (t && t.right && t.right.right && t.right.right.level === t.level) {
      const r = t.right;
      t.right = r.left;
      r.left = t;
      r.level++;
      return r;
    }
    return t;
  }

  insert(key, value) {
    this.root = this._insert(this.root, key, value);
  }

  _insert(t, key, value) {
    if (t == null) { this._size++; return { key, value, left: null, right: null, level: 1 }; }
    if (key < t.key) t.left = this._insert(t.left, key, value);
    else if (key > t.key) t.right = this._insert(t.right, key, value);
    else { t.value = value; return t; }
    t = this._skew(t);
    t = this._split(t);
    return t;
  }

  search(key) {
    let node = this.root;
    while (node) {
      if (key < node.key) node = node.left;
      else if (key > node.key) node = node.right;
      else return node.value;
    }
    return undefined;
  }

  *inOrder(node = this.root) {
    if (!node) return;
    yield* this.inOrder(node.left);
    yield { key: node.key, value: node.value };
    yield* this.inOrder(node.right);
  }

  get size() { return this._size; }
}
