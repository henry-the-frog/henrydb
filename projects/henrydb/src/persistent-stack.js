// persistent-stack.js — Immutable persistent stack
// Push/pop return NEW stacks while preserving old versions.
// O(1) push/pop, O(1) peek, structural sharing via linked list.
// Used in: MVCC, undo/redo, functional programming, transaction logs.

class StackNode {
  constructor(value, next) {
    this.value = value;
    this.next = next;
  }
}

export class PersistentStack {
  constructor(head = null, size = 0) {
    this._head = head;
    this._size = size;
    Object.freeze(this); // Immutable
  }

  get size() { return this._size; }
  get isEmpty() { return this._size === 0; }

  /** Push: returns new stack. O(1). */
  push(value) {
    return new PersistentStack(new StackNode(value, this._head), this._size + 1);
  }

  /** Pop: returns [value, newStack]. O(1). */
  pop() {
    if (!this._head) throw new Error('Stack is empty');
    return [this._head.value, new PersistentStack(this._head.next, this._size - 1)];
  }

  /** Peek at top. O(1). */
  peek() {
    return this._head ? this._head.value : undefined;
  }

  /** Convert to array (top first). */
  toArray() {
    const arr = [];
    let node = this._head;
    while (node) { arr.push(node.value); node = node.next; }
    return arr;
  }

  /** Iterator (top first). */
  *[Symbol.iterator]() {
    let node = this._head;
    while (node) { yield node.value; node = node.next; }
  }

  /** Reverse the stack. O(n). */
  reverse() {
    let result = new PersistentStack();
    for (const value of this) result = result.push(value);
    return result;
  }

  static empty() { return new PersistentStack(); }
}
