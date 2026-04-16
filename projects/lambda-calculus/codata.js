/**
 * Codata and Corecursion
 * 
 * Dual of algebraic data types:
 * - Data: built by constructors, consumed by pattern matching (finite)
 * - Codata: built by observations, consumed by projections (potentially infinite)
 * 
 * Infinite streams, lazy evaluation, coinduction.
 */

// ============================================================
// Streams (infinite lists)
// ============================================================

class Stream {
  constructor(headFn, tailFn) {
    this._head = headFn;
    this._tail = tailFn;
  }
  head() { return typeof this._head === 'function' ? this._head() : this._head; }
  tail() { return typeof this._tail === 'function' ? this._tail() : this._tail; }
}

// Constructors
function cons(h, t) { return new Stream(h, t); }
function repeat(x) { return cons(x, () => repeat(x)); }
function iterate(f, x) { return cons(x, () => iterate(f, f(x))); }
function unfold(f, seed) {
  const [head, nextSeed] = f(seed);
  return cons(head, () => unfold(f, nextSeed));
}
function nats(n = 0) { return iterate(x => x + 1, n); }
function fibs() { return unfold(([a, b]) => [a, [b, a + b]], [0, 1]); }

// ============================================================
// Stream operations (lazy)
// ============================================================

function take(n, stream) {
  const result = [];
  let s = stream;
  for (let i = 0; i < n; i++) {
    result.push(s.head());
    s = s.tail();
  }
  return result;
}

function drop(n, stream) {
  let s = stream;
  for (let i = 0; i < n; i++) s = s.tail();
  return s;
}

function smap(f, stream) {
  return cons(() => f(stream.head()), () => smap(f, stream.tail()));
}

function sfilter(pred, stream) {
  let s = stream;
  while (!pred(s.head())) s = s.tail();
  return cons(s.head(), () => sfilter(pred, s.tail()));
}

function szipWith(f, s1, s2) {
  return cons(() => f(s1.head(), s2.head()), () => szipWith(f, s1.tail(), s2.tail()));
}

function stakeWhile(pred, stream) {
  const result = [];
  let s = stream;
  while (pred(s.head())) {
    result.push(s.head());
    s = s.tail();
  }
  return result;
}

function sinterleave(s1, s2) {
  return cons(s1.head(), () => sinterleave(s2, s1.tail()));
}

// ============================================================
// Coinductive proofs / properties
// ============================================================

// Bisimulation: two streams are bisimilar if they agree on all observations
function bisimilar(s1, s2, n = 100) {
  let a = s1, b = s2;
  for (let i = 0; i < n; i++) {
    if (a.head() !== b.head()) return false;
    a = a.tail();
    b = b.tail();
  }
  return true;
}

export {
  Stream, cons, repeat, iterate, unfold, nats, fibs,
  take, drop, smap, sfilter, szipWith, stakeWhile, sinterleave,
  bisimilar
};
