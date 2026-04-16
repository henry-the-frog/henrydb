/**
 * De Bruijn Reduction — Substitution on Nameless Terms
 * 
 * Implements beta-reduction directly on de Bruijn indexed terms,
 * eliminating the need for alpha-renaming entirely.
 * 
 * Key operations:
 * - Shifting: adjust free variable indices when moving under binders
 * - Substitution: replace index 0 with a term, adjusting indices
 * - Beta-reduction: (λ.M) N → M[0 := shift(N)]
 * 
 * This is how real implementations work (Coq, Lean, etc.)
 */

import {
  DeBruijnVar, DeBruijnAbs, DeBruijnApp,
  toDeBruijn, fromDeBruijn,
} from './lambda.js';

// ============================================================
// Shifting: adjust free variable indices
// shift(d, c, t) — increase all free indices ≥ c by d
// ============================================================

function shift(d, c, term) {
  if (term instanceof DeBruijnVar) {
    return new DeBruijnVar(term.index >= c ? term.index + d : term.index);
  }
  if (term instanceof DeBruijnAbs) {
    return new DeBruijnAbs(shift(d, c + 1, term.body));
  }
  if (term instanceof DeBruijnApp) {
    return new DeBruijnApp(shift(d, c, term.func), shift(d, c, term.arg));
  }
  throw new Error(`Unknown term: ${term}`);
}

// ============================================================
// Substitution: [j → s]t
// Replace all occurrences of index j with term s
// ============================================================

function subst(j, s, term) {
  if (term instanceof DeBruijnVar) {
    return term.index === j ? s : term;
  }
  if (term instanceof DeBruijnAbs) {
    // Under a binder, increase j by 1 and shift s
    return new DeBruijnAbs(subst(j + 1, shift(1, 0, s), term.body));
  }
  if (term instanceof DeBruijnApp) {
    return new DeBruijnApp(subst(j, s, term.func), subst(j, s, term.arg));
  }
  throw new Error(`Unknown term: ${term}`);
}

// ============================================================
// Beta-reduction: (λ.M) N → shift(-1, 0, subst(0, shift(1, 0, N), M))
// ============================================================

function betaReduce(body, arg) {
  // Substitute arg for index 0 in body, then shift down
  return shift(-1, 0, subst(0, shift(1, 0, arg), body));
}

// ============================================================
// Reduction step (normal-order)
// ============================================================

function step(term) {
  // Application of lambda — redex
  if (term instanceof DeBruijnApp && term.func instanceof DeBruijnAbs) {
    return betaReduce(term.func.body, term.arg);
  }
  
  // Try reducing func
  if (term instanceof DeBruijnApp) {
    const f = step(term.func);
    if (f !== null) return new DeBruijnApp(f, term.arg);
    const a = step(term.arg);
    if (a !== null) return new DeBruijnApp(term.func, a);
    return null;
  }
  
  // Reduce under lambda
  if (term instanceof DeBruijnAbs) {
    const b = step(term.body);
    if (b !== null) return new DeBruijnAbs(b);
    return null;
  }
  
  return null;
}

// ============================================================
// Multi-step reduction
// ============================================================

function reduce(term, maxSteps = 1000) {
  let current = term;
  let steps = 0;
  
  while (steps < maxSteps) {
    const next = step(current);
    if (next === null) break;
    current = next;
    steps++;
  }
  
  return { result: current, steps, normalForm: steps < maxSteps };
}

// ============================================================
// Convenience: parse → de Bruijn → reduce → named
// ============================================================

function normalizeDB(term) {
  const db = toDeBruijn(term);
  const result = reduce(db);
  return { ...result, result: fromDeBruijn(result.result) };
}

// ============================================================
// Term size
// ============================================================

function size(term) {
  if (term instanceof DeBruijnVar) return 1;
  if (term instanceof DeBruijnAbs) return 1 + size(term.body);
  if (term instanceof DeBruijnApp) return 1 + size(term.func) + size(term.arg);
  return 0;
}

// ============================================================
// Exports
// ============================================================

export {
  shift, subst, betaReduce,
  step, reduce, normalizeDB,
  size,
};
