/**
 * Higher-Rank Types (Rank-N Polymorphism)
 * 
 * Rank-0: No polymorphism (Int → Int)
 * Rank-1: ∀ only at top level (∀a. a → a)  — Hindley-Milner
 * Rank-2: ∀ in argument position ((∀a. a → a) → Int)
 * Rank-N: ∀ anywhere, arbitrarily nested
 * 
 * Key: at Rank-2+, polymorphic functions can be ARGUMENTS.
 * Example: applyBoth : (∀a. a → a) → (Int, String) → (Int, String)
 *   Takes a function that works for ALL types, and applies it to both Int and String.
 *   You can't do this with Rank-1 because instantiation happens at call site.
 */

// Types
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TForall { constructor(v, body) { this.tag = 'TForall'; this.var = v; this.body = body; } toString() { return `(∀${this.var}. ${this.body})`; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TProd { constructor(l, r) { this.tag = 'TProd'; this.fst = l; this.snd = r; } toString() { return `(${this.fst}, ${this.snd})`; } }

const tInt = new TBase('Int');
const tStr = new TBase('String');
const tBool = new TBase('Bool');

/**
 * Compute the rank of a type
 */
function rank(type) {
  switch (type.tag) {
    case 'TBase': case 'TVar': return 0;
    case 'TForall': return Math.max(1, rank(type.body));
    case 'TFun': {
      const paramRank = rank(type.param);
      const retRank = rank(type.ret);
      // ∀ in negative (argument) position increases rank
      return type.param.tag === 'TForall'
        ? Math.max(paramRank + 1, retRank)
        : Math.max(paramRank, retRank);
    }
    case 'TProd': return Math.max(rank(type.fst), rank(type.snd));
    default: return 0;
  }
}

/**
 * Check if a type is rank-N (for classification)
 */
function classifyRank(type) {
  const r = rank(type);
  if (r === 0) return 'monomorphic';
  if (r === 1) return 'rank-1 (Hindley-Milner)';
  if (r === 2) return 'rank-2';
  return `rank-${r}`;
}

// ============================================================
// Examples of Rank-2 usage (runtime demonstration)
// ============================================================

/**
 * applyBoth : (∀a. a → a) → (Int, String) → (Int, String)
 * This is RANK-2: the argument is polymorphic
 */
function applyBoth(polyFn, pair) {
  return [polyFn(pair[0]), polyFn(pair[1])];
}

/**
 * runST : (∀s. ST s a) → a
 * Classic Rank-2 example: the state token 's' can't escape.
 */
function runST(computation) {
  const stateToken = Symbol('s'); // Fresh, unreachable from outside
  return computation(stateToken);
}

/**
 * withFile : (∀h. Handle h → a) → Filename → a
 * The handle can't escape the callback's scope
 */
function withFile(callback, filename) {
  const handle = { _file: filename, _closed: false };
  const result = callback(handle);
  handle._closed = true; // Close after callback returns
  return result;
}

// ============================================================
// Rank-aware type checking (simplified)
// ============================================================

function canInfer(type) {
  const r = rank(type);
  if (r <= 1) return { inferrable: true, note: 'Standard HM inference' };
  if (r === 2) return { inferrable: true, note: 'Requires annotation on polymorphic arguments' };
  return { inferrable: false, note: `Rank-${r}: type inference is undecidable, annotations required` };
}

function containsForall(type) {
  switch (type.tag) {
    case 'TForall': return true;
    case 'TFun': return containsForall(type.param) || containsForall(type.ret);
    case 'TProd': return containsForall(type.fst) || containsForall(type.snd);
    default: return false;
  }
}

export {
  TVar, TForall, TFun, TBase, TProd,
  tInt, tStr, tBool,
  rank, classifyRank, canInfer, containsForall,
  applyBoth, runST, withFile
};
