/**
 * Curry-Howard Correspondence — Propositions as Types, Proofs as Programs
 * 
 * Demonstrates the isomorphism between:
 * - Propositional logic ↔ Simply Typed Lambda Calculus
 * - Intuitionistic logic ↔ System F
 * 
 * Logical connectives as types:
 * - A → B    (implication)     ↔  function type
 * - A ∧ B    (conjunction)     ↔  product type (pair)
 * - A ∨ B    (disjunction)     ↔  sum type (Either)
 * - ⊤        (truth)           ↔  unit type
 * - ⊥        (falsity)         ↔  empty type (Void)
 * - ∀α. P(α) (universal)      ↔  polymorphic type (System F)
 * 
 * A proof of a proposition is a program of the corresponding type.
 * If you can write the program, the proposition is true!
 */

import {
  TBool, TInt, TUnit, TArrow, TProd,
  TmVar, TmAbs, TmApp, TmBool, TmInt, TmUnit,
  TmIf, TmLet, TmBinOp, TmPair, TmFst, TmSnd,
  typecheck as stlcTypecheck,
} from './stlc.js';

import {
  TVar, TArrow as FTArrow, TForall, TProd as FTProd, TBool as FTBool, TInt as FTInt,
  FVar, FAbs, FApp, FTyAbs, FTyApp, FBool, FInt, FPair, FFst, FSnd,
  typecheck as systemFTypecheck,
} from './systemf.js';

// ============================================================
// Propositions (mirroring types)
// ============================================================

class Prop {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
}

class Implies {
  constructor(ante, cons) { this.ante = ante; this.cons = cons; }
  toString() { return `(${this.ante} → ${this.cons})`; }
}

class And {
  constructor(left, right) { this.left = left; this.right = right; }
  toString() { return `(${this.left} ∧ ${this.right})`; }
}

class Truth {
  toString() { return '⊤'; }
}

class Forall {
  constructor(variable, body) { this.variable = variable; this.body = body; }
  toString() { return `(∀${this.variable}. ${this.body})`; }
}

// ============================================================
// Proposition → Type translation
// ============================================================

function propToSTLC(prop) {
  if (prop instanceof Prop) return new TBool(); // atomic propositions → Bool
  if (prop instanceof Implies) return new TArrow(propToSTLC(prop.ante), propToSTLC(prop.cons));
  if (prop instanceof And) return new TProd(propToSTLC(prop.left), propToSTLC(prop.right));
  if (prop instanceof Truth) return new TUnit();
  throw new Error(`Cannot convert to STLC: ${prop}`);
}

function propToSystemF(prop) {
  if (prop instanceof Prop) return new TVar(prop.name);
  if (prop instanceof Implies) return new FTArrow(propToSystemF(prop.ante), propToSystemF(prop.cons));
  if (prop instanceof And) return new FTProd(propToSystemF(prop.left), propToSystemF(prop.right));
  if (prop instanceof Truth) return new FTBool(); // using Bool as a stand-in
  if (prop instanceof Forall) return new TForall(prop.variable, propToSystemF(prop.body));
  throw new Error(`Cannot convert: ${prop}`);
}

// ============================================================
// Named Theorems with their proofs
// ============================================================

const theorems = {
  // A → A  (identity / reflexivity of implication)
  identity: {
    proposition: 'A → A',
    type: new TArrow(new TBool(), new TBool()),
    proof: new TmAbs('x', new TBool(), new TmVar('x')),
    description: 'Every proposition implies itself',
  },

  // A → (B → A)  (weakening / K combinator)
  weakening: {
    proposition: 'A → (B → A)',
    type: new TArrow(new TInt(), new TArrow(new TBool(), new TInt())),
    proof: new TmAbs('a', new TInt(), new TmAbs('b', new TBool(), new TmVar('a'))),
    description: 'If A is true, then B implies A regardless of B',
  },

  // (A → B) → (B → C) → (A → C)  (transitivity / composition)
  transitivity: {
    proposition: '(A → B) → (B → C) → (A → C)',
    type: new TArrow(
      new TArrow(new TInt(), new TBool()),
      new TArrow(
        new TArrow(new TBool(), new TInt()),
        new TArrow(new TInt(), new TInt()))),
    proof: new TmAbs('f', new TArrow(new TInt(), new TBool()),
      new TmAbs('g', new TArrow(new TBool(), new TInt()),
        new TmAbs('x', new TInt(),
          new TmApp(new TmVar('g'), new TmApp(new TmVar('f'), new TmVar('x')))))),
    description: 'Implication is transitive',
  },

  // A ∧ B → A  (conjunction elimination / fst)
  conjElimLeft: {
    proposition: 'A ∧ B → A',
    type: new TArrow(new TProd(new TInt(), new TBool()), new TInt()),
    proof: new TmAbs('p', new TProd(new TInt(), new TBool()), new TmFst(new TmVar('p'))),
    description: 'From A ∧ B, we can conclude A',
  },

  // A ∧ B → B  (conjunction elimination / snd)
  conjElimRight: {
    proposition: 'A ∧ B → B',
    type: new TArrow(new TProd(new TInt(), new TBool()), new TBool()),
    proof: new TmAbs('p', new TProd(new TInt(), new TBool()), new TmSnd(new TmVar('p'))),
    description: 'From A ∧ B, we can conclude B',
  },

  // A → B → A ∧ B  (conjunction introduction / pair)
  conjIntro: {
    proposition: 'A → B → A ∧ B',
    type: new TArrow(new TInt(), new TArrow(new TBool(), new TProd(new TInt(), new TBool()))),
    proof: new TmAbs('a', new TInt(),
      new TmAbs('b', new TBool(),
        new TmPair(new TmVar('a'), new TmVar('b')))),
    description: 'From A and B, we can conclude A ∧ B',
  },

  // ⊤  (truth is trivially provable)
  truth: {
    proposition: '⊤',
    type: new TUnit(),
    proof: new TmUnit(),
    description: 'Truth is always provable',
  },

  // A ∧ B → B ∧ A  (commutativity of conjunction)
  conjCommute: {
    proposition: 'A ∧ B → B ∧ A',
    type: new TArrow(new TProd(new TInt(), new TBool()), new TProd(new TBool(), new TInt())),
    proof: new TmAbs('p', new TProd(new TInt(), new TBool()),
      new TmPair(new TmSnd(new TmVar('p')), new TmFst(new TmVar('p')))),
    description: 'Conjunction is commutative',
  },

  // (A → B → C) → (A ∧ B → C)  (currying)
  curry: {
    proposition: '(A → B → C) → (A ∧ B → C)',
    type: new TArrow(
      new TArrow(new TInt(), new TArrow(new TBool(), new TInt())),
      new TArrow(new TProd(new TInt(), new TBool()), new TInt())),
    proof: new TmAbs('f', new TArrow(new TInt(), new TArrow(new TBool(), new TInt())),
      new TmAbs('p', new TProd(new TInt(), new TBool()),
        new TmApp(new TmApp(new TmVar('f'), new TmFst(new TmVar('p'))), new TmSnd(new TmVar('p'))))),
    description: 'Currying is a proof of this logical equivalence',
  },

  // (A ∧ B → C) → (A → B → C)  (uncurrying)
  uncurry: {
    proposition: '(A ∧ B → C) → (A → B → C)',
    type: new TArrow(
      new TArrow(new TProd(new TInt(), new TBool()), new TInt()),
      new TArrow(new TInt(), new TArrow(new TBool(), new TInt()))),
    proof: new TmAbs('f', new TArrow(new TProd(new TInt(), new TBool()), new TInt()),
      new TmAbs('a', new TInt(),
        new TmAbs('b', new TBool(),
          new TmApp(new TmVar('f'), new TmPair(new TmVar('a'), new TmVar('b')))))),
    description: 'Uncurrying: the reverse direction',
  },
};

// ============================================================
// System F Theorems (polymorphic / universal propositions)
// ============================================================

const polymorphicTheorems = {
  // ∀α. α → α  (polymorphic identity)
  polyIdentity: {
    proposition: '∀α. α → α',
    proof: new FTyAbs('α', new FAbs('x', new TVar('α'), new FVar('x'))),
    description: 'Parametric polymorphism proves "for all types, the identity holds"',
  },

  // ∀α. ∀β. α → β → α  (polymorphic weakening)
  polyWeakening: {
    proposition: '∀α. ∀β. α → β → α',
    proof: new FTyAbs('α', new FTyAbs('β',
      new FAbs('x', new TVar('α'), new FAbs('y', new TVar('β'), new FVar('x'))))),
    description: 'Universal weakening',
  },

  // ∀α. ∀β. (α → β) → (∀γ. (β → γ) → (α → γ))
  // This is a polymorphic version of transitivity
  polyTransitivity: {
    proposition: '∀α. ∀β. (α → β) → ∀γ. (β → γ) → α → γ',
    proof: new FTyAbs('α', new FTyAbs('β',
      new FAbs('f', new FTArrow(new TVar('α'), new TVar('β')),
        new FTyAbs('γ',
          new FAbs('g', new FTArrow(new TVar('β'), new TVar('γ')),
            new FAbs('x', new TVar('α'),
              new FApp(new FVar('g'), new FApp(new FVar('f'), new FVar('x'))))))))),
    description: 'Polymorphic transitivity of implication',
  },
};

// ============================================================
// Verify all proofs typecheck
// ============================================================

function verifyAllProofs() {
  const results = [];
  
  for (const [name, thm] of Object.entries(theorems)) {
    try {
      const inferredType = stlcTypecheck(thm.proof);
      const matches = inferredType.equals(thm.type);
      results.push({ name, proposition: thm.proposition, valid: matches, error: null });
    } catch (e) {
      results.push({ name, proposition: thm.proposition, valid: false, error: e.message });
    }
  }
  
  for (const [name, thm] of Object.entries(polymorphicTheorems)) {
    try {
      systemFTypecheck(thm.proof);
      results.push({ name, proposition: thm.proposition, valid: true, error: null });
    } catch (e) {
      results.push({ name, proposition: thm.proposition, valid: false, error: e.message });
    }
  }
  
  return results;
}

// ============================================================
// Exports
// ============================================================

export {
  // Propositions
  Prop, Implies, And, Truth, Forall,
  // Conversion
  propToSTLC, propToSystemF,
  // Theorems
  theorems, polymorphicTheorems,
  // Verification
  verifyAllProofs,
};
