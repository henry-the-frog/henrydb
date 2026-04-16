/**
 * Type-Level Computation
 * 
 * Computations that happen entirely at the type level during type checking.
 * Like TypeScript's conditional types, Haskell's type families, or C++ template metaprogramming.
 * 
 * Features:
 * - Type-level naturals (Peano: Zero, Succ)
 * - Type-level booleans (True, False)
 * - Type-level lists (Nil, Cons)
 * - Type-level functions (type families)
 * - Type-level conditionals
 * - Type-level arithmetic (Add, Mul, comparison)
 * - Dependent-style Vec (length-indexed lists)
 */

// ============================================================
// Type-Level Values
// ============================================================

class TLZero {
  constructor() { this.tag = 'TLZero'; }
  toString() { return 'Zero'; }
  toNumber() { return 0; }
}

class TLSucc {
  constructor(pred) { this.tag = 'TLSucc'; this.pred = pred; }
  toString() { return `Succ(${this.pred})`; }
  toNumber() { return 1 + this.pred.toNumber(); }
}

class TLTrue {
  constructor() { this.tag = 'TLTrue'; }
  toString() { return 'True'; }
}

class TLFalse {
  constructor() { this.tag = 'TLFalse'; }
  toString() { return 'False'; }
}

class TLNil {
  constructor() { this.tag = 'TLNil'; }
  toString() { return 'Nil'; }
}

class TLCons {
  constructor(head, tail) { this.tag = 'TLCons'; this.head = head; this.tail = tail; }
  toString() { return `Cons(${this.head}, ${this.tail})`; }
}

// Base types for elements
class TLBase {
  constructor(name) { this.tag = 'TLBase'; this.name = name; }
  toString() { return this.name; }
}

// Vec: length-indexed list
class TLVec {
  constructor(elemType, length) { this.tag = 'TLVec'; this.elemType = elemType; this.length = length; }
  toString() { return `Vec(${this.elemType}, ${this.length})`; }
}

// ============================================================
// Convenience constructors
// ============================================================

const zero = new TLZero();
const one = new TLSucc(zero);
const two = new TLSucc(one);
const three = new TLSucc(two);
const four = new TLSucc(three);
const five = new TLSucc(four);

function nat(n) {
  let result = zero;
  for (let i = 0; i < n; i++) result = new TLSucc(result);
  return result;
}

const tlTrue = new TLTrue();
const tlFalse = new TLFalse();
const tlNil = new TLNil();

function cons(h, t) { return new TLCons(h, t); }
function list(...elems) {
  let result = tlNil;
  for (let i = elems.length - 1; i >= 0; i--) result = cons(elems[i], result);
  return result;
}

// ============================================================
// Type-Level Functions (Type Families)
// ============================================================

/** Type-level addition: Add(Zero, m) = m; Add(Succ(n), m) = Succ(Add(n, m)) */
function tlAdd(a, b) {
  if (a.tag === 'TLZero') return b;
  if (a.tag === 'TLSucc') return new TLSucc(tlAdd(a.pred, b));
  throw new Error(`tlAdd: expected natural, got ${a.tag}`);
}

/** Type-level multiplication: Mul(Zero, m) = Zero; Mul(Succ(n), m) = Add(m, Mul(n, m)) */
function tlMul(a, b) {
  if (a.tag === 'TLZero') return zero;
  if (a.tag === 'TLSucc') return tlAdd(b, tlMul(a.pred, b));
  throw new Error(`tlMul: expected natural, got ${a.tag}`);
}

/** Type-level equality */
function tlEqual(a, b) {
  if (a.tag === 'TLZero' && b.tag === 'TLZero') return tlTrue;
  if (a.tag === 'TLSucc' && b.tag === 'TLSucc') return tlEqual(a.pred, b.pred);
  return tlFalse;
}

/** Type-level less-than */
function tlLessThan(a, b) {
  if (a.tag === 'TLZero' && b.tag === 'TLSucc') return tlTrue;
  if (b.tag === 'TLZero') return tlFalse;
  if (a.tag === 'TLSucc' && b.tag === 'TLSucc') return tlLessThan(a.pred, b.pred);
  return tlFalse;
}

/** Type-level conditional: If(True, then, else) = then; If(False, then, else) = else */
function tlIf(cond, thenType, elseType) {
  if (cond.tag === 'TLTrue') return thenType;
  if (cond.tag === 'TLFalse') return elseType;
  throw new Error(`tlIf: expected boolean, got ${cond.tag}`);
}

/** Type-level NOT */
function tlNot(b) {
  if (b.tag === 'TLTrue') return tlFalse;
  if (b.tag === 'TLFalse') return tlTrue;
  throw new Error(`tlNot: expected boolean, got ${b.tag}`);
}

/** Type-level AND */
function tlAnd(a, b) {
  return tlIf(a, b, tlFalse);
}

/** Type-level OR */
function tlOr(a, b) {
  return tlIf(a, tlTrue, b);
}

// ============================================================
// Type-Level List Operations
// ============================================================

/** Length of a type-level list */
function tlLength(list) {
  if (list.tag === 'TLNil') return zero;
  if (list.tag === 'TLCons') return new TLSucc(tlLength(list.tail));
  throw new Error(`tlLength: expected list, got ${list.tag}`);
}

/** Append two type-level lists */
function tlAppend(a, b) {
  if (a.tag === 'TLNil') return b;
  if (a.tag === 'TLCons') return cons(a.head, tlAppend(a.tail, b));
  throw new Error(`tlAppend: expected list, got ${a.tag}`);
}

/** Reverse a type-level list */
function tlReverse(lst, acc = tlNil) {
  if (lst.tag === 'TLNil') return acc;
  if (lst.tag === 'TLCons') return tlReverse(lst.tail, cons(lst.head, acc));
  throw new Error(`tlReverse: expected list, got ${lst.tag}`);
}

/** Map a type-level function over a list */
function tlMap(fn, lst) {
  if (lst.tag === 'TLNil') return tlNil;
  if (lst.tag === 'TLCons') return cons(fn(lst.head), tlMap(fn, lst.tail));
  throw new Error(`tlMap: expected list, got ${lst.tag}`);
}

/** Filter a type-level list */
function tlFilter(pred, lst) {
  if (lst.tag === 'TLNil') return tlNil;
  if (lst.tag === 'TLCons') {
    const keep = pred(lst.head);
    const rest = tlFilter(pred, lst.tail);
    if (keep.tag === 'TLTrue') return cons(lst.head, rest);
    return rest;
  }
  throw new Error(`tlFilter: expected list, got ${lst.tag}`);
}

// ============================================================
// Vec Operations (Length-indexed)
// ============================================================

/** Create a Vec from elements */
function vec(elemType, ...elems) {
  return new TLVec(elemType, nat(elems.length));
}

/** Append two Vecs: Vec(a, n) ++ Vec(a, m) = Vec(a, n+m) */
function vecAppend(v1, v2) {
  if (v1.elemType.name !== v2.elemType.name) throw new Error('Vec element type mismatch');
  return new TLVec(v1.elemType, tlAdd(v1.length, v2.length));
}

/** Head of Vec: requires length >= 1 (checked at type level) */
function vecHead(v) {
  if (tlEqual(v.length, zero).tag === 'TLTrue') {
    throw new Error('Type error: cannot take head of Vec(_, Zero)');
  }
  return v.elemType;
}

/** Tail of Vec(a, Succ(n)) = Vec(a, n) */
function vecTail(v) {
  if (v.length.tag !== 'TLSucc') {
    throw new Error('Type error: cannot take tail of Vec(_, Zero)');
  }
  return new TLVec(v.elemType, v.length.pred);
}

// ============================================================
// Type-Level Arithmetic: Subtraction, Min, Max
// ============================================================

/** Subtraction (saturating): Sub(Zero, _) = Zero; Sub(n, Zero) = n; Sub(Succ(n), Succ(m)) = Sub(n, m) */
function tlSub(a, b) {
  if (a.tag === 'TLZero') return zero;
  if (b.tag === 'TLZero') return a;
  if (a.tag === 'TLSucc' && b.tag === 'TLSucc') return tlSub(a.pred, b.pred);
  throw new Error(`tlSub: invalid args`);
}

/** Min of two naturals */
function tlMin(a, b) {
  return tlIf(tlLessThan(a, b), a, b);
}

/** Max of two naturals */
function tlMax(a, b) {
  return tlIf(tlLessThan(a, b), b, a);
}

/** Is even? */
function tlIsEven(n) {
  if (n.tag === 'TLZero') return tlTrue;
  if (n.tag === 'TLSucc') {
    if (n.pred.tag === 'TLZero') return tlFalse;
    if (n.pred.tag === 'TLSucc') return tlIsEven(n.pred.pred);
  }
  throw new Error(`tlIsEven: invalid arg`);
}

// ============================================================
// Type equality
// ============================================================

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'TLZero': return true;
    case 'TLSucc': return typeEquals(a.pred, b.pred);
    case 'TLTrue': case 'TLFalse': case 'TLNil': return true;
    case 'TLCons': return typeEquals(a.head, b.head) && typeEquals(a.tail, b.tail);
    case 'TLBase': return a.name === b.name;
    case 'TLVec': return typeEquals(a.elemType, b.elemType) && typeEquals(a.length, b.length);
    default: return false;
  }
}

// ============================================================
// Exports
// ============================================================

export {
  TLZero, TLSucc, TLTrue, TLFalse, TLNil, TLCons, TLBase, TLVec,
  zero, one, two, three, four, five, nat,
  tlTrue, tlFalse, tlNil, cons, list,
  tlAdd, tlMul, tlEqual, tlLessThan,
  tlIf, tlNot, tlAnd, tlOr,
  tlLength, tlAppend, tlReverse, tlMap, tlFilter,
  vec, vecAppend, vecHead, vecTail,
  tlSub, tlMin, tlMax, tlIsEven,
  typeEquals
};
