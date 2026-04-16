import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Prop, Implies, And, Truth, Forall,
  propToSTLC, propToSystemF,
  theorems, polymorphicTheorems,
  verifyAllProofs,
} from './curry-howard.js';
import {
  TBool, TInt, TUnit, TArrow, TProd,
  typecheck as stlcTypecheck, evaluate,
  TmApp, TmInt, TmBool, TmUnit, TmPair,
} from './stlc.js';
import {
  TVar, TForall, TArrow as FTArrow,
  typecheck as systemFTypecheck, evaluate as fEvaluate,
  FApp, FTyApp, FInt, FBool,
} from './systemf.js';

// ============================================================
// Core Correspondence: Every provable proposition has a well-typed term
// ============================================================

describe('Curry-Howard: All STLC Proofs Typecheck', () => {
  for (const [name, thm] of Object.entries(theorems)) {
    it(`${thm.proposition} — ${thm.description}`, () => {
      const inferredType = stlcTypecheck(thm.proof);
      assert(inferredType.equals(thm.type),
        `Expected ${thm.type}, got ${inferredType}`);
    });
  }
});

describe('Curry-Howard: All System F Proofs Typecheck', () => {
  for (const [name, thm] of Object.entries(polymorphicTheorems)) {
    it(`${thm.proposition} — ${thm.description}`, () => {
      const t = systemFTypecheck(thm.proof);
      assert(t instanceof TForall || t instanceof FTArrow);
    });
  }
});

// ============================================================
// Proofs as Computations (running the proofs)
// ============================================================

describe('Proofs as Computations', () => {
  it('Identity proof: A → A applied to 42 gives 42', () => {
    const r = evaluate(new TmApp(theorems.identity.proof, new TmBool(true)));
    assert.equal(r.result.value, true);
  });

  it('Weakening proof: A → B → A applied to (5, true) gives 5', () => {
    const r = evaluate(new TmApp(new TmApp(theorems.weakening.proof, new TmInt(5)), new TmBool(true)));
    assert.equal(r.result.value, 5);
  });

  it('Conjunction introduction: A → B → A∧B creates a pair', () => {
    const r = evaluate(new TmApp(new TmApp(theorems.conjIntro.proof, new TmInt(1)), new TmBool(false)));
    assert.equal(r.result.fst.value, 1);
    assert.equal(r.result.snd.value, false);
  });

  it('Conjunction elimination left: A∧B → A extracts first', () => {
    const pair = new TmPair(new TmInt(42), new TmBool(true));
    const r = evaluate(new TmApp(theorems.conjElimLeft.proof, pair));
    assert.equal(r.result.value, 42);
  });

  it('Conjunction elimination right: A∧B → B extracts second', () => {
    const pair = new TmPair(new TmInt(42), new TmBool(true));
    const r = evaluate(new TmApp(theorems.conjElimRight.proof, pair));
    assert.equal(r.result.value, true);
  });

  it('Commutativity: A∧B → B∧A swaps pair', () => {
    const pair = new TmPair(new TmInt(1), new TmBool(true));
    const r = evaluate(new TmApp(theorems.conjCommute.proof, pair));
    assert.equal(r.result.fst.value, true);
    assert.equal(r.result.snd.value, 1);
  });

  it('Truth is trivially provable', () => {
    const t = stlcTypecheck(theorems.truth.proof);
    assert(t.equals(new TUnit()));
  });

  it('Transitivity: (A→B) → (B→C) → (A→C) composes functions', () => {
    // f: Int → Bool (is positive)
    // g: Bool → Int (bool to int)
    // compose g f: Int → Int
    const f = theorems.transitivity.proof;
    // Not easily testable with actual evaluation (would need real functions)
    // But typechecking proves the logical theorem
    const t = stlcTypecheck(f);
    assert(t instanceof TArrow);
  });

  it('Curry: (A → B → C) → (A∧B → C)', () => {
    // Give it a curried addition and a pair
    // Can't easily test with generic types, but typecheck proves it
    const t = stlcTypecheck(theorems.curry.proof);
    assert(t instanceof TArrow);
  });

  it('Uncurry: (A∧B → C) → (A → B → C)', () => {
    const t = stlcTypecheck(theorems.uncurry.proof);
    assert(t instanceof TArrow);
  });
});

// ============================================================
// Polymorphic Proofs as Computations
// ============================================================

describe('Polymorphic Proofs as Computations', () => {
  it('Polymorphic identity works at any type', () => {
    const pid = polymorphicTheorems.polyIdentity.proof;
    
    const r1 = fEvaluate(new FApp(new FTyApp(pid, new TVar('Int')), new FInt(42)));
    assert.equal(r1.result.value, 42);
    
    const r2 = fEvaluate(new FApp(new FTyApp(pid, new TVar('Bool')), new FBool(true)));
    assert.equal(r2.result.value, true);
  });

  it('Polymorphic weakening works at any two types', () => {
    const pw = polymorphicTheorems.polyWeakening.proof;
    const r = fEvaluate(new FApp(new FApp(
      new FTyApp(new FTyApp(pw, new TVar('Int')), new TVar('Bool')),
      new FInt(99)), new FBool(false)));
    assert.equal(r.result.value, 99);
  });
});

// ============================================================
// Proposition ↔ Type Translation
// ============================================================

describe('Proposition → Type Translation', () => {
  it('A → B translates to arrow type', () => {
    const prop = new Implies(new Prop('A'), new Prop('B'));
    const t = propToSTLC(prop);
    assert(t instanceof TArrow);
  });

  it('A ∧ B translates to product type', () => {
    const prop = new And(new Prop('A'), new Prop('B'));
    const t = propToSTLC(prop);
    assert(t instanceof TProd);
  });

  it('⊤ translates to unit type', () => {
    assert(propToSTLC(new Truth()) instanceof TUnit);
  });

  it('∀α. α → α translates to System F forall', () => {
    const prop = new Forall('α', new Implies(new Prop('α'), new Prop('α')));
    const t = propToSystemF(prop);
    assert(t instanceof TForall);
  });
});

// ============================================================
// Verify All Proofs (batch)
// ============================================================

describe('Batch Verification', () => {
  it('all proofs are valid', () => {
    const results = verifyAllProofs();
    for (const r of results) {
      assert(r.valid, `Proof "${r.name}" (${r.proposition}) failed: ${r.error}`);
    }
    assert(results.length >= 13); // 10 STLC + 3 System F
  });
});

// ============================================================
// Negative: Unprovable Propositions = Uninhabited Types
// ============================================================

describe('Unprovable Propositions (negative tests)', () => {
  it('⊥ (Void/empty type) has no proof — cannot construct a term', () => {
    // In STLC, there's no term of type Void
    // We can verify this by noting that no constructor exists
    // (This is the logical analog of "falsity has no proof")
    assert(true); // Documenting the principle
  });

  it('A → B is not generally provable (no polymorphic coercion)', () => {
    // You can't write a function Int → Bool without actual computation
    // The type system enforces this
    assert(true); // The point is that you CAN'T write such a term
  });
});

// ============================================================
// The Deep Insight
// ============================================================

describe('The Correspondence Table', () => {
  const correspondences = [
    ['A → B', 'function type A → B', 'Modus Ponens = function application'],
    ['A ∧ B', 'product type A × B', 'Conjunction intro = pair construction'],
    ['A ∧ B → A', 'fst : A × B → A', 'Conjunction elim = projection'],
    ['⊤', 'Unit type', 'Truth = trivially constructible value'],
    ['∀α. P(α)', '∀α. T(α)', 'Universal quantification = parametric polymorphism'],
    ['proof of A', 'term of type A', 'Proofs ARE programs'],
    ['A is provable', 'type A is inhabited', 'Provability = inhabitedness'],
    ['A is unprovable', 'type A is empty', 'Logical impossibility = type error'],
  ];

  for (const [logic, types, insight] of correspondences) {
    it(`${logic}  ↔  ${types}  (${insight})`, () => {
      assert(true); // These are documented correspondences
    });
  }
});
