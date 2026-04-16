/**
 * Recursive Types: μα.T
 * 
 * Two approaches:
 * 1. Equirecursive: μα.T = T[α := μα.T] (types are equal, automatic unrolling)
 * 2. Iso-recursive: μα.T ≅ T[α := μα.T] (need explicit fold/unfold)
 * 
 * Examples:
 * - Nat = μα. 1 + α  (zero or successor)
 * - List a = μα. 1 + (a × α)  (nil or cons)
 * - Tree a = μα. a + (α × α)  (leaf or branch)
 */

// Type constructors
class TMu { constructor(v, body) { this.tag = 'TMu'; this.var = v; this.body = body; } toString() { return `μ${this.var}.${this.body}`; } }
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TSum { constructor(l, r) { this.tag = 'TSum'; this.left = l; this.right = r; } toString() { return `(${this.left} + ${this.right})`; } }
class TProd { constructor(l, r) { this.tag = 'TProd'; this.left = l; this.right = r; } toString() { return `(${this.left} × ${this.right})`; } }
class TUnit { constructor() { this.tag = 'TUnit'; } toString() { return '1'; } }
class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }

const tUnit = new TUnit();
const tInt = new TBase('Int');
const tStr = new TBase('Str');

// Standard recursive types
const tNat = new TMu('α', new TSum(tUnit, new TVar('α')));
const tIntList = new TMu('α', new TSum(tUnit, new TProd(tInt, new TVar('α'))));
const tIntTree = new TMu('α', new TSum(tInt, new TProd(new TVar('α'), new TVar('α'))));

// ============================================================
// Equirecursive: automatic unrolling
// ============================================================

function substitute(type, varName, replacement) {
  switch (type.tag) {
    case 'TVar': return type.name === varName ? replacement : type;
    case 'TMu': return type.var === varName ? type : new TMu(type.var, substitute(type.body, varName, replacement));
    case 'TSum': return new TSum(substitute(type.left, varName, replacement), substitute(type.right, varName, replacement));
    case 'TProd': return new TProd(substitute(type.left, varName, replacement), substitute(type.right, varName, replacement));
    default: return type;
  }
}

function unroll(muType) {
  if (muType.tag !== 'TMu') throw new Error('Expected μ-type');
  return substitute(muType.body, muType.var, muType);
}

// Equirecursive equality (coinductive, with seen-pairs to handle cycles)
function equiEqual(t1, t2, seen = new Set()) {
  const key = `${t1}=${t2}`;
  if (seen.has(key)) return true; // Assumed equal (coinductive hypothesis)
  seen.add(key);
  
  // Unfold μ-types
  if (t1.tag === 'TMu') return equiEqual(unroll(t1), t2, seen);
  if (t2.tag === 'TMu') return equiEqual(t1, unroll(t2), seen);
  
  if (t1.tag !== t2.tag) return false;
  
  switch (t1.tag) {
    case 'TUnit': return true;
    case 'TBase': return t1.name === t2.name;
    case 'TVar': return t1.name === t2.name;
    case 'TSum': return equiEqual(t1.left, t2.left, seen) && equiEqual(t1.right, t2.right, seen);
    case 'TProd': return equiEqual(t1.left, t2.left, seen) && equiEqual(t1.right, t2.right, seen);
    default: return false;
  }
}

// ============================================================
// Iso-recursive: explicit fold/unfold
// ============================================================

class Fold {
  constructor(muType, value) { this.tag = 'Fold'; this.muType = muType; this.value = value; }
}

function fold(muType, value) { return new Fold(muType, value); }
function unfold(folded) {
  if (folded.tag !== 'Fold') throw new Error('Expected folded value');
  return folded.value;
}

// ============================================================
// Value constructors using fold/unfold
// ============================================================

// Nat values
function zero() { return fold(tNat, { tag: 'Left', value: null }); }
function succ(n) { return fold(tNat, { tag: 'Right', value: n }); }

function natToInt(n) {
  let count = 0;
  let current = n;
  while (true) {
    const inner = unfold(current);
    if (inner.tag === 'Left') return count;
    count++;
    current = inner.value;
  }
}

function intToNat(n) {
  let result = zero();
  for (let i = 0; i < n; i++) result = succ(result);
  return result;
}

// List values
function nil() { return fold(tIntList, { tag: 'Left', value: null }); }
function cons(head, tail) { return fold(tIntList, { tag: 'Right', value: { fst: head, snd: tail } }); }

function listToArray(lst) {
  const result = [];
  let current = lst;
  while (true) {
    const inner = unfold(current);
    if (inner.tag === 'Left') return result;
    result.push(inner.value.fst);
    current = inner.value.snd;
  }
}

// Tree values
function leaf(n) { return fold(tIntTree, { tag: 'Left', value: n }); }
function branch(left, right) { return fold(tIntTree, { tag: 'Right', value: { fst: left, snd: right } }); }

function treeSum(t) {
  const inner = unfold(t);
  if (inner.tag === 'Left') return inner.value;
  return treeSum(inner.value.fst) + treeSum(inner.value.snd);
}

export {
  TMu, TVar, TSum, TProd, TUnit, TBase,
  tUnit, tInt, tStr, tNat, tIntList, tIntTree,
  substitute, unroll, equiEqual,
  fold, unfold, Fold,
  zero, succ, natToInt, intToNat,
  nil, cons, listToArray,
  leaf, branch, treeSum
};
