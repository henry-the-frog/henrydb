/**
 * Parametricity and Free Theorems (Wadler 1989)
 * 
 * "Theorems for free!": derive properties from types alone.
 * 
 * For any function f : ∀a. [a] → [a]:
 *   map g ∘ f = f ∘ map g    (f commutes with map)
 * 
 * This means f can only rearrange/select elements, never inspect them.
 * (shuffle, reverse, take n, filter by position — but NOT sort, because sort needs Ord)
 */

// ============================================================
// Free Theorem Generator
// ============================================================

function freeTheorem(type) {
  switch (type.tag) {
    case 'TForall': {
      // ∀a. T  →  for any relation R on a, the free theorem of T[a↦R] holds
      return {
        theorem: `For all types A, B and relation R: A ↔ B`,
        body: freeTheorem(type.body),
        quantifier: type.var
      };
    }
    
    case 'TFun': {
      // (A → B)  →  if R_A(a₁,a₂) then R_B(f(a₁), f(a₂))
      return {
        theorem: `If R_input(x₁, x₂) then R_output(f(x₁), f(x₂))`,
        input: type.param,
        output: type.ret
      };
    }
    
    case 'TList': {
      return { theorem: `map R commutes: map(r, f(xs)) = f(map(r, xs))` };
    }
    
    case 'TVar': {
      return { theorem: `Related by the relation for ${type.name}` };
    }
    
    default:
      return { theorem: 'No interesting free theorem' };
  }
}

// Type constructors
class TForall { constructor(v, body) { this.tag = 'TForall'; this.var = v; this.body = body; } toString() { return `∀${this.var}. ${this.body}`; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TList { constructor(elem) { this.tag = 'TList'; this.elem = elem; } toString() { return `[${this.elem}]`; } }
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TPair { constructor(a, b) { this.tag = 'TPair'; this.fst = a; this.snd = b; } toString() { return `(${this.fst}, ${this.snd})`; } }
class TCon { constructor(name) { this.tag = 'TCon'; this.name = name; } toString() { return this.name; } }

const tInt = new TCon('Int');

// ============================================================
// Verify free theorems empirically
// ============================================================

/**
 * Verify: for f : [a] → [a], map(g, f(xs)) === f(map(g, xs))
 */
function verifyListFreeTheorem(f, g, xs) {
  const left = f(xs).map(g);          // map g (f xs)
  const right = f(xs.map(g));          // f (map g xs)
  return JSON.stringify(left) === JSON.stringify(right);
}

/**
 * Verify: for f : a → a → a, g(f(x,y)) === f(g(x), g(y))
 * Only holds for specific f (like const, projections)
 */
function verifyBinaryFreeTheorem(f, g, x, y) {
  const left = g(f(x, y));
  const right = f(g(x), g(y));
  return left === right;
}

/**
 * Generate possible implementations of ∀a. [a] → [a]
 * (only rearrangement functions are valid)
 */
const validListFunctions = {
  identity: xs => xs,
  reverse: xs => [...xs].reverse(),
  tail: xs => xs.slice(1),
  init: xs => xs.slice(0, -1),
  take2: xs => xs.slice(0, 2),
  drop1: xs => xs.slice(1),
  duplicate: xs => [...xs, ...xs],
  empty: xs => [],
  singleton: xs => xs.length > 0 ? [xs[0]] : [],
};

// These are NOT valid ∀a. [a] → [a] (they inspect elements)
const invalidListFunctions = {
  sort: xs => [...xs].sort(),          // Needs Ord a
  nub: xs => [...new Set(xs)],         // Needs Eq a
  filter: xs => xs.filter(x => x > 0), // Needs Ord a
};

export {
  TForall, TFun, TList, TVar, TPair, TCon, tInt,
  freeTheorem,
  verifyListFreeTheorem, verifyBinaryFreeTheorem,
  validListFunctions, invalidListFunctions
};
