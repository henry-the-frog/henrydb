/**
 * Logical Relations: Prove program equivalence via relational interpretation
 * 
 * Logical relations are the workhorse of PL metatheory:
 * - Type safety (Progress + Preservation)
 * - Parametricity (free theorems)
 * - Compiler correctness (source ≈ target)
 * - Normalization (all terms terminate)
 */

// Basic type universe
class TBase { constructor(n) { this.tag = 'TBase'; this.name = n; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } }

// Define a logical relation at each type
function logicalRelation(type, rel) {
  switch (type.tag) {
    case 'TBase': return rel[type.name] || ((a, b) => a === b); // Base: use provided relation
    case 'TFun': {
      // Functions: related if they map related inputs to related outputs
      // (f,g) ∈ R[A→B] iff ∀(a,b) ∈ R[A]. (f(a), g(b)) ∈ R[B]
      const paramRel = logicalRelation(type.param, rel);
      const retRel = logicalRelation(type.ret, rel);
      return (f, g) => {
        // Test with sample inputs from the relation
        return { paramRel, retRel, check: (a, b) => {
          if (!paramRel(a, b)) return false;
          return retRel(f(a), g(b));
        }};
      };
    }
  }
}

// Contextual equivalence: two terms are equivalent if no context can distinguish them
function contextuallyEquivalent(term1, term2, contexts) {
  return contexts.every(ctx => ctx(term1) === ctx(term2));
}

// Observational equivalence: agree on all observations
function observationallyEquivalent(term1, term2, observations) {
  return observations.every(obs => obs(term1) === obs(term2));
}

// Adequacy: if related then observationally equivalent
function adequacy(rel, term1, term2, observations) {
  const related = rel(term1, term2);
  const obsEquiv = observationallyEquivalent(term1, term2, observations);
  return { related, obsEquiv, adequate: !related || obsEquiv }; // related ⟹ obsEquiv
}

// Fundamental theorem: well-typed terms are in the logical relation
function fundamentalTheorem(term, type, rel) {
  const r = logicalRelation(type, rel);
  return r(term, term); // Self-relation (identity extension)
}

// Step-indexed logical relation (for recursive types)
function stepIndexed(n, type, rel) {
  if (n <= 0) return () => true; // At step 0, everything is related
  return logicalRelation(type, rel);
}

export { TBase, TFun, logicalRelation, contextuallyEquivalent, observationallyEquivalent, adequacy, fundamentalTheorem, stepIndexed };
