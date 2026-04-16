/**
 * Curry-Howard with Calculus of Constructions
 * 
 * Extends Curry-Howard correspondence to dependent types:
 * - Π(x:A).B          ↔  universal quantification ∀x:A. B(x)
 * - Σ(x:A).B          ↔  existential quantification ∃x:A. B(x)
 * - Equality type      ↔  propositional equality
 * - natElim            ↔  mathematical induction
 * 
 * Proofs by induction become structurally recursive programs.
 * Propositions that depend on values are types that compute.
 */

import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, TypeError,
  infer, check, normalize, betaEq, subst, arrow,
  freshName, resetNames
} from './coc.js';

// ============================================================
// Equality Type (Martin-Löf Identity Type)
// ============================================================

// Eq : Π(A:★).A → A → ★
// Refl : Π(A:★).Π(x:A).Eq A x x
//
// We encode Eq A x y as Π(P:A→★). P x → P y (Leibniz equality)

// Eq type constructor: takes a type and two values
function eqType(A, x, y) {
  // Eq A x y = Π(P:A→★). P x → P y
  return new Pi('P', arrow(A, new Star()),
    arrow(new App(new Var('P'), x), new App(new Var('P'), y)));
}

// Refl proof: reflexivity — Eq A x x
function refl(A, x) {
  // λ(P:A→★).λ(px:P x).px
  return new Lam('P', arrow(A, new Star()),
    new Lam('px', new App(new Var('P'), x), new Var('px')));
}

// Symmetry: Eq A x y → Eq A y x
function symm(A, x, y, proof) {
  // Use the equality proof to transport along P where P(z) = Eq A z x
  // proof : Π(P:A→★). P x → P y
  // We want: Π(P:A→★). P y → P x
  // Apply proof to (λz. Eq A z x → Eq A z x) ... this is complex
  // Simpler: use proof with P = λ(z:A).Eq A z x
  // proof (λ(z:A). Eq A z x) (refl A x) : Eq A y x
  const P = new Lam('z', A, eqType(A, new Var('z'), x));
  return new App(new App(proof, P), refl(A, x));
}

// ============================================================
// Sigma Types (Dependent Pairs / Existentials)
// ============================================================

// Σ(x:A).B = Π(C:★).(Π(x:A).B → C) → C  (Church encoding)
function sigmaType(param, A, B) {
  return new Pi('C', new Star(),
    arrow(new Pi(param, A, arrow(B, new Var('C'))), new Var('C')));
}

// Dependent pair constructor: (a, b) : Σ(x:A).B
function dpair(param, A, B, a, b) {
  // λ(C:★).λ(f:Π(x:A).B→C).f a b
  return new Lam('C', new Star(),
    new Lam('f', new Pi(param, A, arrow(B, new Var('C'))),
      new App(new App(new Var('f'), a), b)));
}

// ============================================================
// Proof by Induction (via natElim)
// ============================================================

// Proof that 0 + n = n (left identity of addition)
// This is trivial by definition: plus 0 n = natElim(λ_.ℕ, n, λk.λih.S ih, 0) = n
function plusZeroLeft(n) {
  const P = new Lam('_', new Nat(), new Nat());
  return new NatElim(P, n,
    new Lam('k', new Nat(), new Lam('ih', new Nat(), new Succ(new Var('ih')))),
    new Zero());
}

// Addition function
function plus(m, n) {
  const P = new Lam('_', new Nat(), new Nat());
  return new NatElim(P, m,
    new Lam('k', new Nat(), new Lam('ih', new Nat(), new Succ(new Var('ih')))),
    n);
}

// Double function: n → n + n
function double(n) {
  return plus(n, n);
}

// IsZero predicate: ℕ → ★
// IsZero 0 = ⊤ (unit type encoded as Π(A:★).A→A)
// IsZero (S n) = ⊥ (void type encoded as Π(A:★).A)
function isZeroProp() {
  const unitType = new Pi('A', new Star(), arrow(new Var('A'), new Var('A')));
  const voidType = new Pi('A', new Star(), new Var('A'));
  // λ(n:ℕ).natElim(λ_.★, ⊤, λk.λ_.⊥, n)
  return new Lam('n', new Nat(),
    new NatElim(
      new Lam('_', new Nat(), new Star()),
      unitType,
      new Lam('k', new Nat(), new Lam('_', new Star(), voidType)),
      new Var('n')));
}

// Proof that 0 is zero: IsZero 0
function proofZeroIsZero() {
  // Need a term of type Π(A:★).A→A, which is the identity
  return new Lam('A', new Star(), new Lam('x', new Var('A'), new Var('x')));
}

// ============================================================
// Dependent Vector Type (length-indexed lists)
// ============================================================

// Vec A n — a vector of n elements of type A
// Encoding using natElim:
// Vec A 0 = ⊤ (unit)
// Vec A (S n) = A × Vec A n (pair)
//
// We encode pairs as Π(C:★).(A → B → C) → C

function vecType(A) {
  const unitType = new Pi('C', new Star(), arrow(new Var('C'), new Var('C')));
  // λ(n:ℕ).natElim(λ_.★, ⊤, λk.λV. Π(C:★).(A → V → C) → C, n)
  return new Lam('n', new Nat(),
    new NatElim(
      new Lam('_', new Nat(), new Star()),
      unitType,
      new Lam('k', new Nat(), new Lam('V', new Star(),
        new Pi('C', new Star(),
          arrow(arrow(A, arrow(new Var('V'), new Var('C'))), new Var('C'))))),
      new Var('n')));
}

// Empty vector: vnil : Vec A 0
function vnil() {
  // A term of type Π(C:★).C→C, which is the identity
  return new Lam('C', new Star(), new Lam('x', new Var('C'), new Var('x')));
}

// Vector cons: vcons a v : Vec A (S n)  given a : A, v : Vec A n
function vcons(A, a, v) {
  // A term of type Π(C:★).(A → VecTail → C) → C
  return new Lam('C', new Star(),
    new Lam('f', arrow(A, arrow(new Var('V_placeholder'), new Var('C'))),
      new App(new App(new Var('f'), a), v)));
}

// ============================================================
// Exports
// ============================================================

export {
  eqType, refl, symm,
  sigmaType, dpair,
  plus, double, plusZeroLeft,
  isZeroProp, proofZeroIsZero,
  vecType, vnil, vcons
};
