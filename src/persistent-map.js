// persistent-map.js — Persistent (immutable) sorted map with structural sharing
// Functional red-black tree: insert/delete return new tree, old tree unchanged.

const RED = true, BLACK = false;

function node(key, value, left, right, color) {
  return { key, value, left, right, color };
}

function isRed(n) { return n != null && n.color === RED; }

function rotateLeft(h) {
  const x = node(h.right.key, h.right.value, node(h.key, h.value, h.left, h.right.left, RED), h.right.right, h.color);
  return x;
}

function rotateRight(h) {
  const x = node(h.left.key, h.left.value, h.left.left, node(h.key, h.value, h.left.right, h.right, RED), h.color);
  return x;
}

function flipColors(h) {
  return node(h.key, h.value,
    h.left ? node(h.left.key, h.left.value, h.left.left, h.left.right, !h.left.color) : null,
    h.right ? node(h.right.key, h.right.value, h.right.left, h.right.right, !h.right.color) : null,
    !h.color);
}

function fixUp(h) {
  if (isRed(h.right) && !isRed(h.left)) h = rotateLeft(h);
  if (isRed(h.left) && isRed(h.left?.left)) h = rotateRight(h);
  if (isRed(h.left) && isRed(h.right)) h = flipColors(h);
  return h;
}

function insertNode(h, key, value) {
  if (h == null) return node(key, value, null, null, RED);
  
  if (key < h.key) h = node(h.key, h.value, insertNode(h.left, key, value), h.right, h.color);
  else if (key > h.key) h = node(h.key, h.value, h.left, insertNode(h.right, key, value), h.color);
  else h = node(h.key, value, h.left, h.right, h.color); // Update
  
  return fixUp(h);
}

function searchNode(h, key) {
  while (h) {
    if (key < h.key) h = h.left;
    else if (key > h.key) h = h.right;
    else return h.value;
  }
  return undefined;
}

function* inOrderNode(h) {
  if (!h) return;
  yield* inOrderNode(h.left);
  yield [h.key, h.value];
  yield* inOrderNode(h.right);
}

function sizeNode(h) { return h == null ? 0 : 1 + sizeNode(h.left) + sizeNode(h.right); }

export class PersistentSortedMap {
  constructor(root = null) { this._root = root; }

  /** Returns a NEW map with key inserted */
  set(key, value) {
    let r = insertNode(this._root, key, value);
    r = node(r.key, r.value, r.left, r.right, BLACK); // Root always black
    return new PersistentSortedMap(r);
  }

  get(key) { return searchNode(this._root, key); }
  has(key) { return this.get(key) !== undefined; }

  *entries() { yield* inOrderNode(this._root); }
  *keys() { for (const [k] of this.entries()) yield k; }
  *values() { for (const [, v] of this.entries()) yield v; }

  get size() { return sizeNode(this._root); }
  
  toObject() {
    const obj = {};
    for (const [k, v] of this.entries()) obj[k] = v;
    return obj;
  }
}
