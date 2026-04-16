/**
 * Domain Theory Basics
 * 
 * Mathematical foundations for denotational semantics.
 * - Pointed CPOs (complete partial orders with bottom element ⊥)
 * - Scott-continuous functions (preserve directed suprema)
 * - Fixed-point semantics (Y = fix f = f(f(f(...))))
 * 
 * This gives rigorous meaning to recursion and infinite computation.
 */

// ============================================================
// Flat Domain: {⊥, v₁, v₂, ...}
// ============================================================

const BOT = Symbol('⊥');

class FlatDomain {
  constructor(name, elements) {
    this.name = name;
    this.elements = elements; // Set of values (not including ⊥)
  }

  // Partial order: ⊥ ⊑ x for all x, and x ⊑ x (reflexive)
  leq(a, b) {
    if (a === BOT) return true;
    return a === b;
  }

  // Least upper bound (for flat domain: defined only if a = b or one is ⊥)
  lub(a, b) {
    if (a === BOT) return b;
    if (b === BOT) return a;
    if (a === b) return a;
    throw new Error(`No LUB for ${a} and ${b} in flat domain`);
  }

  bottom() { return BOT; }
}

// ============================================================
// Lifting: add ⊥ to any set
// ============================================================

class LiftedDomain {
  constructor(values) {
    this.values = values; // [v1, v2, ...]
  }

  leq(a, b) {
    if (a === BOT) return true;
    return a === b;
  }

  lub(a, b) {
    if (a === BOT) return b;
    if (b === BOT) return a;
    if (a === b) return a;
    return undefined; // Inconsistent
  }

  bottom() { return BOT; }
  isBottom(x) { return x === BOT; }
}

// ============================================================
// Product Domain: D₁ × D₂
// ============================================================

class ProductDomain {
  constructor(d1, d2) { this.d1 = d1; this.d2 = d2; }

  leq([a1, a2], [b1, b2]) {
    return this.d1.leq(a1, b1) && this.d2.leq(a2, b2);
  }

  lub([a1, a2], [b1, b2]) {
    return [this.d1.lub(a1, b1), this.d2.lub(a2, b2)];
  }

  bottom() { return [this.d1.bottom(), this.d2.bottom()]; }
}

// ============================================================
// Scott-continuous function application
// ============================================================

/**
 * A Scott-continuous function preserves directed suprema:
 *   f(⊔S) = ⊔{f(s) | s ∈ S}
 * In practice: monotone + preserves limits of increasing chains
 */
class ScottFn {
  constructor(fn) { this.fn = fn; }
  apply(x) { return this.fn(x); }
}

// ============================================================
// Fixed-point computation (Kleene's theorem)
// ============================================================

/**
 * Least fixed point: fix f = ⊔{fⁿ(⊥) | n ∈ ℕ}
 * Compute by iterating f from ⊥ until convergence.
 */
function fix(f, bottom, maxIter = 100, eq = (a, b) => a === b) {
  let current = bottom;
  for (let i = 0; i < maxIter; i++) {
    const next = f(current);
    if (eq(current, next)) return current;
    current = next;
  }
  return current; // Approximation after maxIter
}

/**
 * Kleene chain: [⊥, f(⊥), f²(⊥), ...]
 */
function kleeneChain(f, bottom, n) {
  const chain = [bottom];
  let current = bottom;
  for (let i = 0; i < n; i++) {
    current = f(current);
    chain.push(current);
  }
  return chain;
}

// ============================================================
// Denotational semantics of a simple language
// ============================================================

/**
 * Meaning of factorial in domain theory:
 *   [[fact]] = fix(λf.λn. if n=0 then 1 else n*f(n-1))
 */
function factDenotation() {
  return fix(
    f => n => n === 0 ? 1 : (f === BOT ? BOT : n * f(n - 1)),
    BOT,
    100,
    (a, b) => {
      // Compare functions by testing on inputs 0-10
      if (a === BOT && b === BOT) return true;
      if (a === BOT || b === BOT) return false;
      for (let i = 0; i <= 10; i++) {
        if (a(i) !== b(i)) return false;
      }
      return true;
    }
  );
}

export {
  BOT, FlatDomain, LiftedDomain, ProductDomain,
  ScottFn, fix, kleeneChain, factDenotation
};
