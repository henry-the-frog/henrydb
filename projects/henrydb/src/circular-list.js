// circular-list.js — Circular doubly-linked list
export class CircularList {
  constructor() { this._head = null; this._size = 0; }
  get size() { return this._size; }

  add(value) {
    const node = { value, prev: null, next: null };
    if (!this._head) { node.prev = node; node.next = node; this._head = node; }
    else { node.prev = this._head.prev; node.next = this._head; this._head.prev.next = node; this._head.prev = node; }
    this._size++;
    return node;
  }

  remove(node) {
    if (this._size === 1) { this._head = null; }
    else { node.prev.next = node.next; node.next.prev = node.prev; if (node === this._head) this._head = node.next; }
    this._size--;
  }

  *[Symbol.iterator]() {
    if (!this._head) return;
    let node = this._head;
    do { yield node.value; node = node.next; } while (node !== this._head);
  }
}
