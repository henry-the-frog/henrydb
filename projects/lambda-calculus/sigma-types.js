/**
 * Dependent Pairs (Sigma Types): Σ(x:A).B(x)
 * 
 * A pair where the TYPE of the second component depends on the VALUE of the first.
 * 
 * Examples:
 * - Σ(n:Nat). Vec n  → "a number n paired with a vector of exactly n elements"
 * - Σ(b:Bool). if b then Int else Str → "a bool with type depending on its value"
 * 
 * This is existential quantification in type theory:
 * Σ(x:A).B(x) packages a witness x with proof that B(x) holds.
 */

// Type constructors
class TSigma { constructor(v, fstType, sndType) { this.tag = 'TSigma'; this.var = v; this.fst = fstType; this.snd = sndType; } toString() { return `Σ(${this.var}:${this.fst}).${this.snd}`; } }
class TPi { constructor(v, domain, codomain) { this.tag = 'TPi'; this.var = v; this.domain = domain; this.codomain = codomain; } toString() { return `Π(${this.var}:${this.domain}).${this.codomain}`; } }
class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TVec { constructor(elem, len) { this.tag = 'TVec'; this.elem = elem; this.len = len; } toString() { return `Vec(${this.elem}, ${this.len})`; } }
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }

const tNat = new TBase('Nat');
const tInt = new TBase('Int');
const tStr = new TBase('Str');
const tBool = new TBase('Bool');

// ============================================================
// Dependent pair values
// ============================================================

class DPair {
  constructor(fst, snd, type) {
    this.fst = fst;
    this.snd = snd;
    this.type = type; // TSigma
  }
  
  toString() { return `⟨${this.fst}, ${this.snd}⟩`; }
}

/**
 * Create a dependent pair with type checking
 */
function dpair(fst, snd, sigmaType, checkFn) {
  if (checkFn && !checkFn(fst, snd)) {
    throw new Error(`Type error: second component doesn't match dependency on first`);
  }
  return new DPair(fst, snd, sigmaType);
}

/**
 * Project first component (always safe)
 */
function fst(pair) { return pair.fst; }

/**
 * Project second component (type depends on first!)
 */
function snd(pair) { return pair.snd; }

// ============================================================
// Length-indexed vectors
// ============================================================

class Vec {
  constructor(elements) { this.elements = elements; }
  get length() { return this.elements.length; }
  get(i) { return this.elements[i]; }
  toString() { return `Vec[${this.elements.join(', ')}]`; }
}

/**
 * mkVec: create a Σ(n:Nat). Vec(Int, n) — dependent pair of length and vector
 */
function mkVec(elements) {
  const v = new Vec(elements);
  return dpair(v.length, v, null, (n, vec) => vec.length === n);
}

/**
 * concat: Σ(n:Nat).Vec n → Σ(m:Nat).Vec m → Σ(n+m:Nat).Vec(n+m)
 */
function vecConcat(pair1, pair2) {
  const n = fst(pair1);
  const m = fst(pair2);
  const combined = [...snd(pair1).elements, ...snd(pair2).elements];
  return mkVec(combined);
}

// ============================================================
// Conditional types via Sigma
// ============================================================

/**
 * Dependent conditional: Σ(b:Bool). if b then A else B
 */
function depCond(b, thenVal, elseVal) {
  return dpair(b, b ? thenVal : elseVal, null);
}

/**
 * Match on dependent conditional
 */
function matchDepCond(pair, ifTrue, ifFalse) {
  return fst(pair) ? ifTrue(snd(pair)) : ifFalse(snd(pair));
}

// ============================================================
// Type-level functions
// ============================================================

/**
 * Apply a type-level function: given a value, compute the dependent type
 */
function depType(value, typeFamily) {
  return typeFamily(value);
}

export {
  TSigma, TPi, TBase, TVec, TVar,
  tNat, tInt, tStr, tBool,
  DPair, dpair, fst, snd,
  Vec, mkVec, vecConcat,
  depCond, matchDepCond, depType
};
