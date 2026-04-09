// rope.js — Rope data structure for efficient large string operations
// Binary tree where leaves hold string chunks. O(log n) insert/delete
// at any position. Used in text editors (Xi editor, Atom).

class RopeNode {
  constructor(text) {
    this.text = text || null;  // Only for leaves
    this.left = null;
    this.right = null;
    this.weight = text ? text.length : 0; // Length of left subtree
    this.length = text ? text.length : 0;
  }
}

export class Rope {
  constructor(text = '') {
    this._root = text.length > 0 ? this._build(text) : null;
  }

  get length() { return this._root ? this._root.length : 0; }

  /** Get character at index. O(log n). */
  charAt(index) {
    return this._charAt(this._root, index);
  }

  /** Get substring. O(log n + k). */
  substring(start, end) {
    if (!this._root) return '';
    const chars = [];
    this._substring(this._root, start, end, chars);
    return chars.join('');
  }

  /** Insert text at position. O(log n). */
  insert(pos, text) {
    const newNode = new RopeNode(text);
    if (!this._root) { this._root = newNode; return; }
    const [left, right] = this._split(this._root, pos);
    this._root = this._concat(this._concat(left, newNode), right);
  }

  /** Delete range [start, end). O(log n). */
  delete(start, end) {
    if (!this._root) return;
    const [left, rest] = this._split(this._root, start);
    const [_, right] = this._split(rest, end - start);
    this._root = this._concat(left, right);
  }

  /** Concatenate another rope. O(log n). */
  append(text) {
    const newNode = typeof text === 'string' ? new RopeNode(text) : text._root;
    this._root = this._concat(this._root, newNode);
  }

  /** Convert to string. O(n). */
  toString() {
    return this.substring(0, this.length);
  }

  // --- Internal ---

  _build(text, chunkSize = 256) {
    if (text.length <= chunkSize) return new RopeNode(text);
    const mid = Math.floor(text.length / 2);
    const node = new RopeNode();
    node.left = this._build(text.slice(0, mid), chunkSize);
    node.right = this._build(text.slice(mid), chunkSize);
    node.weight = node.left.length;
    node.length = node.left.length + node.right.length;
    return node;
  }

  _charAt(node, index) {
    if (!node) return '';
    if (node.text) return node.text[index] || '';
    if (index < node.weight) return this._charAt(node.left, index);
    return this._charAt(node.right, index - node.weight);
  }

  _substring(node, start, end, chars) {
    if (!node || start >= end) return;
    if (node.text) {
      const s = Math.max(0, start);
      const e = Math.min(node.text.length, end);
      if (s < e) chars.push(node.text.slice(s, e));
      return;
    }
    if (start < node.weight) {
      this._substring(node.left, start, Math.min(end, node.weight), chars);
    }
    if (end > node.weight) {
      this._substring(node.right, Math.max(0, start - node.weight), end - node.weight, chars);
    }
  }

  _split(node, pos) {
    if (!node) return [null, null];
    if (node.text) {
      if (pos <= 0) return [null, node];
      if (pos >= node.text.length) return [node, null];
      return [new RopeNode(node.text.slice(0, pos)), new RopeNode(node.text.slice(pos))];
    }
    if (pos <= node.weight) {
      const [ll, lr] = this._split(node.left, pos);
      return [ll, this._concat(lr, node.right)];
    }
    const [rl, rr] = this._split(node.right, pos - node.weight);
    return [this._concat(node.left, rl), rr];
  }

  _concat(left, right) {
    if (!left) return right;
    if (!right) return left;
    const node = new RopeNode();
    node.left = left;
    node.right = right;
    node.weight = left.length;
    node.length = left.length + right.length;
    return node;
  }
}
