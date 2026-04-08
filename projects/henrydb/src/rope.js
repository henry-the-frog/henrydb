// rope.js — Rope data structure for efficient string operations
// Binary tree of string fragments; O(log n) concat, split, charAt.

export class Rope {
  constructor(str) {
    if (typeof str === 'string') {
      this.left = null;
      this.right = null;
      this.str = str;
      this.len = str.length;
    } else {
      // Internal node
      this.left = str.left;
      this.right = str.right;
      this.str = null;
      this.len = (str.left?.len || 0) + (str.right?.len || 0);
    }
  }

  get length() { return this.len; }

  /** Concatenate two ropes */
  static concat(a, b) {
    if (!a) return b;
    if (!b) return a;
    return new Rope({ left: a, right: b });
  }

  /** Character at position */
  charAt(idx) {
    if (idx < 0 || idx >= this.len) return undefined;
    if (this.str != null) return this.str[idx];
    const leftLen = this.left?.len || 0;
    if (idx < leftLen) return this.left.charAt(idx);
    return this.right.charAt(idx - leftLen);
  }

  /** Split at position */
  split(pos) {
    if (pos <= 0) return [null, this];
    if (pos >= this.len) return [this, null];
    
    if (this.str != null) {
      return [new Rope(this.str.slice(0, pos)), new Rope(this.str.slice(pos))];
    }
    
    const leftLen = this.left?.len || 0;
    if (pos < leftLen) {
      const [ll, lr] = this.left.split(pos);
      return [ll, Rope.concat(lr, this.right)];
    } else if (pos > leftLen) {
      const [rl, rr] = this.right.split(pos - leftLen);
      return [Rope.concat(this.left, rl), rr];
    } else {
      return [this.left, this.right];
    }
  }

  /** Insert string at position */
  insert(pos, str) {
    const piece = new Rope(str);
    const [left, right] = this.split(pos);
    return Rope.concat(Rope.concat(left, piece), right);
  }

  /** Delete range [start, end) */
  delete(start, end) {
    const [left, rest] = this.split(start);
    const [, right] = rest.split(end - start);
    return Rope.concat(left, right);
  }

  /** Substring */
  substring(start, end) {
    const chars = [];
    for (let i = start; i < end && i < this.len; i++) chars.push(this.charAt(i));
    return chars.join('');
  }

  /** Convert to plain string */
  toString() {
    if (this.str != null) return this.str;
    return (this.left?.toString() || '') + (this.right?.toString() || '');
  }

  /** Tree depth */
  depth() {
    if (this.str != null) return 0;
    return 1 + Math.max(this.left?.depth() || 0, this.right?.depth() || 0);
  }
}
