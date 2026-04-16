/**
 * Proof Irrelevance & Type Erasure
 * 
 * Some type information exists only for verification — it has no computational content.
 * At runtime, these types can be ERASED without changing program behavior.
 * 
 * Examples:
 * - Phantom type parameters (already computed at compile time)
 * - Proof terms in dependently typed languages
 * - Coercions in Haskell (newtype wrappers)
 * 
 * Key idea: separate the "relevant" (computational) parts from the
 * "irrelevant" (proof/type) parts, then erase the irrelevant ones.
 */

// Relevance annotations
const RELEVANT = 'relevant';       // Used at runtime
const IRRELEVANT = 'irrelevant';   // Erased at runtime (proofs, types)
const SHAPE = 'shape';             // Only shape matters (not values)

// Annotated types
class TAnnotated {
  constructor(type, relevance) {
    this.type = type;
    this.relevance = relevance;
  }
  toString() {
    const mark = this.relevance === IRRELEVANT ? '⁰' : this.relevance === SHAPE ? '¹' : '';
    return `${this.type}${mark}`;
  }
}

// AST with relevance
class EVar { constructor(name, rel = RELEVANT) { this.tag = 'EVar'; this.name = name; this.rel = rel; } }
class ELam { constructor(v, rel, body) { this.tag = 'ELam'; this.var = v; this.rel = rel; this.body = body; } }
class EApp { constructor(fn, arg, rel = RELEVANT) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; this.rel = rel; } }
class ELet { constructor(v, rel, init, body) { this.tag = 'ELet'; this.var = v; this.rel = rel; this.init = init; this.body = body; } }
class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } }
class EType { constructor(t) { this.tag = 'EType'; this.type = t; } } // Type as value (erased)

// ============================================================
// Type erasure
// ============================================================

function erase(expr) {
  switch (expr.tag) {
    case 'ENum': return expr;
    case 'EVar': return expr.rel === IRRELEVANT ? { tag: 'EUnit' } : expr;
    case 'ELam':
      if (expr.rel === IRRELEVANT) return erase(expr.body); // Erase irrelevant lambda
      return { tag: 'ELam', var: expr.var, body: erase(expr.body) };
    case 'EApp':
      if (expr.rel === IRRELEVANT) return erase(expr.fn); // Erase irrelevant application
      return { tag: 'EApp', fn: erase(expr.fn), arg: erase(expr.arg) };
    case 'ELet':
      if (expr.rel === IRRELEVANT) return erase(expr.body);
      return { tag: 'ELet', var: expr.var, init: erase(expr.init), body: erase(expr.body) };
    case 'EType': return { tag: 'EUnit' }; // Types erased completely
    default: return expr;
  }
}

// ============================================================
// Size analysis: how much smaller after erasure?
// ============================================================

function nodeCount(expr) {
  if (!expr || typeof expr !== 'object') return 0;
  switch (expr.tag) {
    case 'ENum': case 'EVar': case 'EUnit': case 'EType': return 1;
    case 'ELam': return 1 + nodeCount(expr.body);
    case 'EApp': return 1 + nodeCount(expr.fn) + nodeCount(expr.arg);
    case 'ELet': return 1 + nodeCount(expr.init) + nodeCount(expr.body);
    default: return 1;
  }
}

function erasureStats(expr) {
  const before = nodeCount(expr);
  const erased = erase(expr);
  const after = nodeCount(erased);
  return {
    before,
    after,
    erased: before - after,
    ratio: before > 0 ? ((before - after) / before * 100).toFixed(1) + '%' : '0%'
  };
}

// ============================================================
// Newtype coercions (zero-cost wrappers)
// ============================================================

class Newtype {
  constructor(name, inner) { this.name = name; this.inner = inner; }
  wrap(value) { return value; }   // Zero-cost: same representation
  unwrap(value) { return value; } // Zero-cost: same representation
  coerce(value) { return value; }
}

export {
  RELEVANT, IRRELEVANT, SHAPE,
  TAnnotated, EVar, ELam, EApp, ELet, ENum, EType,
  erase, nodeCount, erasureStats, Newtype
};
