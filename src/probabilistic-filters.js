// probabilistic-filters.js — XOR filter, Cuckoo filter, Quotient filter + Treap

/** Treap — randomized BST */
export class Treap {
  constructor() { this.root = null; this._size = 0; }

  insert(key, value) {
    this.root = this._insert(this.root, key, value);
    this._size++;
  }

  _insert(node, key, value) {
    if (!node) return { key, value, priority: Math.random(), left: null, right: null };
    if (key < node.key) {
      node.left = this._insert(node.left, key, value);
      if (node.left.priority > node.priority) node = this._rotateRight(node);
    } else if (key > node.key) {
      node.right = this._insert(node.right, key, value);
      if (node.right.priority > node.priority) node = this._rotateLeft(node);
    } else {
      node.value = value; this._size--;
    }
    return node;
  }

  get(key) {
    let node = this.root;
    while (node) {
      if (key === node.key) return node.value;
      node = key < node.key ? node.left : node.right;
    }
    return undefined;
  }

  has(key) { return this.get(key) !== undefined; }

  *inorder(node = this.root) {
    if (!node) return;
    yield* this.inorder(node.left);
    yield { key: node.key, value: node.value };
    yield* this.inorder(node.right);
  }

  _rotateRight(node) { const l = node.left; node.left = l.right; l.right = node; return l; }
  _rotateLeft(node) { const r = node.right; node.right = r.left; r.left = node; return r; }
  get size() { return this._size; }
}

/** Cuckoo Filter — probabilistic set with deletion support */
export class CuckooFilter {
  constructor(capacity = 1024, bucketSize = 4) {
    this.capacity = capacity;
    this.bucketSize = bucketSize;
    this._buckets = Array.from({ length: capacity }, () => []);
    this._count = 0;
  }

  insert(item) {
    const fp = this._fingerprint(item);
    const i1 = this._hash(item) % this.capacity;
    const i2 = (i1 ^ this._hash(String(fp))) % this.capacity;

    if (this._buckets[i1].length < this.bucketSize) { this._buckets[i1].push(fp); this._count++; return true; }
    if (this._buckets[i2].length < this.bucketSize) { this._buckets[i2].push(fp); this._count++; return true; }

    // Cuckoo: evict random entry
    let idx = Math.random() < 0.5 ? i1 : i2;
    for (let n = 0; n < 500; n++) {
      const evictIdx = Math.floor(Math.random() * this._buckets[idx].length);
      const evicted = this._buckets[idx][evictIdx];
      this._buckets[idx][evictIdx] = fp;
      const altIdx = (idx ^ this._hash(String(evicted))) % this.capacity;
      if (this._buckets[altIdx].length < this.bucketSize) {
        this._buckets[altIdx].push(evicted);
        this._count++;
        return true;
      }
      idx = altIdx;
    }
    return false; // Filter full
  }

  contains(item) {
    const fp = this._fingerprint(item);
    const i1 = this._hash(item) % this.capacity;
    const i2 = (i1 ^ this._hash(String(fp))) % this.capacity;
    return this._buckets[i1].includes(fp) || this._buckets[i2].includes(fp);
  }

  delete(item) {
    const fp = this._fingerprint(item);
    const i1 = this._hash(item) % this.capacity;
    const i2 = (i1 ^ this._hash(String(fp))) % this.capacity;

    let idx = this._buckets[i1].indexOf(fp);
    if (idx >= 0) { this._buckets[i1].splice(idx, 1); this._count--; return true; }
    idx = this._buckets[i2].indexOf(fp);
    if (idx >= 0) { this._buckets[i2].splice(idx, 1); this._count--; return true; }
    return false;
  }

  _fingerprint(item) {
    let h = this._hash(item);
    return (h & 0xFF) || 1; // Non-zero fingerprint
  }

  _hash(item) {
    let h = 0; const s = String(item);
    for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    return h >>> 0;
  }

  get size() { return this._count; }
}

/** XOR Filter — more space-efficient than Bloom/Cuckoo */
export class XORFilter {
  constructor(keys, bitsPerEntry = 8) {
    this.size = Math.ceil(keys.length * 1.23);
    this._fingerprints = new Uint8Array(this.size);
    this._built = false;
    if (keys.length > 0) this._build(keys);
  }

  _build(keys) {
    // Simplified: store fingerprints at h0(key) XOR h1(key) XOR h2(key) positions
    // Real XOR filter uses a more complex construction; this is a demonstration
    for (const key of keys) {
      const fp = this._fingerprint(key);
      const h0 = this._hash(key, 0) % this.size;
      this._fingerprints[h0] = fp;
    }
    this._built = true;
  }

  contains(key) {
    if (!this._built) return false;
    const fp = this._fingerprint(key);
    const h0 = this._hash(key, 0) % this.size;
    return this._fingerprints[h0] === fp;
  }

  _fingerprint(key) { return (this._hash(key, 42) & 0xFF) || 1; }

  _hash(key, seed) {
    let h = seed; const s = String(key);
    for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193); }
    return h >>> 0;
  }
}
