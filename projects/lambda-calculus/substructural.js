/**
 * Substructural Types: Ordered, Affine, Relevant, Linear
 * 
 * Classical logic has three structural rules:
 * 1. Weakening: can ignore (not use) a variable
 * 2. Contraction: can use a variable multiple times
 * 3. Exchange: can reorder variables
 * 
 * Remove combinations to get substructural systems:
 * - Linear: no weakening, no contraction (use exactly once)
 * - Affine: no contraction (use at most once) 
 * - Relevant: no weakening (use at least once)
 * - Ordered: no weakening, no contraction, no exchange (stack discipline)
 * - Unrestricted: all rules (classical)
 */

const ORDERED = 'ordered';         // No W, No C, No E
const LINEAR = 'linear';           // No W, No C
const AFFINE = 'affine';           // No C
const RELEVANT = 'relevant';       // No W
const UNRESTRICTED = 'unrestricted'; // All rules

// Structural rules available
const RULES = {
  [ORDERED]:      { weakening: false, contraction: false, exchange: false },
  [LINEAR]:       { weakening: false, contraction: false, exchange: true },
  [AFFINE]:       { weakening: true,  contraction: false, exchange: true },
  [RELEVANT]:     { weakening: false, contraction: true,  exchange: true },
  [UNRESTRICTED]: { weakening: true,  contraction: true,  exchange: true },
};

class SubstructuralChecker {
  constructor(mode) {
    this.mode = mode;
    this.rules = RULES[mode];
    this.errors = [];
  }

  check(usages) {
    // usages: Map<varName, count>
    for (const [name, count] of usages) {
      if (count === 0 && !this.rules.weakening) {
        this.errors.push(`${name}: unused (weakening not allowed in ${this.mode})`);
      }
      if (count > 1 && !this.rules.contraction) {
        this.errors.push(`${name}: used ${count} times (contraction not allowed in ${this.mode})`);
      }
    }
    return { ok: this.errors.length === 0, errors: this.errors };
  }

  checkOrder(usageOrder, declOrder) {
    if (!this.rules.exchange) {
      // Must use in declaration order
      for (let i = 0; i < usageOrder.length - 1; i++) {
        const a = declOrder.indexOf(usageOrder[i]);
        const b = declOrder.indexOf(usageOrder[i + 1]);
        if (a > b) {
          this.errors.push(`${usageOrder[i]} used before ${usageOrder[i + 1]} but declared after (exchange not allowed in ${this.mode})`);
        }
      }
    }
    return { ok: this.errors.length === 0, errors: this.errors };
  }
}

// Subtyping between modalities
function isSubMode(m1, m2) {
  const r1 = RULES[m1], r2 = RULES[m2];
  // m1 <: m2 if m1 is MORE restrictive (fewer rules)
  return (!r1.weakening || r2.weakening) &&
         (!r1.contraction || r2.contraction) &&
         (!r1.exchange || r2.exchange);
}

// Combine modalities (join in the lattice)
function joinMode(m1, m2) {
  if (m1 === m2) return m1;
  if (isSubMode(m1, m2)) return m2;
  if (isSubMode(m2, m1)) return m1;
  return UNRESTRICTED; // Least upper bound
}

export {
  ORDERED, LINEAR, AFFINE, RELEVANT, UNRESTRICTED, RULES,
  SubstructuralChecker, isSubMode, joinMode
};
