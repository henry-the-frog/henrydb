/**
 * Inductive Types for Calculus of Constructions
 * 
 * Extends CoC with user-defined inductive types that generate:
 * 1. Type former (the type itself)
 * 2. Constructors (introduction rules)
 * 3. Eliminator (elimination/recursion principle)
 * 
 * Based on the Calculus of Inductive Constructions (CIC),
 * the type theory underlying Coq and Lean.
 */

import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, TypeError,
  infer, check, normalize, betaEq, subst, arrow,
  freshName, resetNames
} from './coc.js';

// ============================================================
// Inductive Type Definition
// ============================================================

/**
 * Define an inductive type.
 * 
 * @param {string} name - Type name (e.g., "Bool", "List")
 * @param {Array} params - Type parameters [{name, type}] (e.g., [{name: "A", type: Star}] for List)
 * @param {Array} constructors - [{name, argTypes}] where argTypes are functions of params
 * @returns {object} { type, constructors, eliminator, eliminatorType }
 */
function defineInductive(name, params, constructors) {
  // Church-encoding approach: 
  // An inductive type is encoded as its elimination principle.
  // T = Π(C:★). (constructor1_shape → C) → ... → (constructorN_shape → C) → C
  
  const result = {
    name,
    params,
    constructors: {},
    eliminatorName: `${name.toLowerCase()}Elim`,
  };
  
  // Build the Church-encoded type
  // For each param, we have a top-level Pi binding
  // Then Π(C:★). (case1 → C) → (case2 → C) → ... → C
  
  function buildType(paramBindings) {
    const cVar = new Var('C');
    let body = cVar; // final result is C
    
    // Build constructor cases right-to-left
    for (let i = constructors.length - 1; i >= 0; i--) {
      const ctor = constructors[i];
      const argTypes = ctor.argTypes(paramBindings);
      // case_i = arg1 → arg2 → ... → C
      let caseType = cVar;
      for (let j = argTypes.length - 1; j >= 0; j--) {
        caseType = arrow(argTypes[j], caseType);
      }
      body = arrow(caseType, body);
    }
    
    // Wrap in Π(C:★)
    body = new Pi('C', new Star(), body);
    
    // Wrap in param Pis
    for (let i = params.length - 1; i >= 0; i--) {
      body = new Pi(params[i].name, params[i].type, body);
    }
    
    return body;
  }
  
  result.type = buildType(Object.fromEntries(params.map(p => [p.name, new Var(p.name)])));
  
  // Build constructors
  // Constructor i selects the i-th case
  for (let i = 0; i < constructors.length; i++) {
    const ctor = constructors[i];
    const paramBindings = Object.fromEntries(params.map(p => [p.name, new Var(p.name)]));
    const argTypes = ctor.argTypes(paramBindings);
    
    // Constructor: λ(params...).λ(args...).λ(C:★).λ(cases...).case_i(args...)
    let body = new Var(`case_${i}`);
    
    // Apply constructor arguments to the case
    for (let j = 0; j < argTypes.length; j++) {
      body = new App(body, new Var(`arg_${j}`));
    }
    
    // Wrap in case lambdas (right to left)
    for (let k = constructors.length - 1; k >= 0; k--) {
      const caseArgTypes = constructors[k].argTypes(paramBindings);
      let caseType = new Var('C');
      for (let j = caseArgTypes.length - 1; j >= 0; j--) {
        caseType = arrow(caseArgTypes[j], caseType);
      }
      body = new Lam(`case_${k}`, caseType, body);
    }
    
    // Wrap in Π(C:★)
    body = new Lam('C', new Star(), body);
    
    // Wrap in argument lambdas
    for (let j = argTypes.length - 1; j >= 0; j--) {
      body = new Lam(`arg_${j}`, argTypes[j], body);
    }
    
    // Wrap in param lambdas
    for (let k = params.length - 1; k >= 0; k--) {
      body = new Lam(params[k].name, params[k].type, body);
    }
    
    result.constructors[ctor.name] = body;
  }
  
  return result;
}

// ============================================================
// Pre-defined Inductive Types
// ============================================================

/**
 * Bool = Π(C:★). C → C → C
 * true = λ(C:★).λ(t:C).λ(f:C).t
 * false = λ(C:★).λ(t:C).λ(f:C).f
 */
function defineBool() {
  return defineInductive('Bool', [], [
    { name: 'true', argTypes: () => [] },
    { name: 'false', argTypes: () => [] },
  ]);
}

/**
 * Maybe A = Π(C:★). C → (A → C) → C
 * nothing = λ(A:★).λ(C:★).λ(n:C).λ(j:A→C).n
 * just = λ(A:★).λ(x:A).λ(C:★).λ(n:C).λ(j:A→C).j x
 */
function defineMaybe() {
  return defineInductive('Maybe', [{ name: 'A', type: new Star() }], [
    { name: 'nothing', argTypes: () => [] },
    { name: 'just', argTypes: (p) => [p.A] },
  ]);
}

/**
 * Either A B = Π(C:★). (A → C) → (B → C) → C
 * left = λ(A:★).λ(B:★).λ(x:A).λ(C:★).λ(l:A→C).λ(r:B→C).l x
 * right = λ(A:★).λ(B:★).λ(x:B).λ(C:★).λ(l:A→C).λ(r:B→C).r x
 */
function defineEither() {
  return defineInductive('Either', [
    { name: 'A', type: new Star() },
    { name: 'B', type: new Star() },
  ], [
    { name: 'left', argTypes: (p) => [p.A] },
    { name: 'right', argTypes: (p) => [p.B] },
  ]);
}

/**
 * List A = Π(C:★). C → (A → C → C) → C
 * nil = λ(A:★).λ(C:★).λ(n:C).λ(c:A→C→C).n
 * cons = λ(A:★).λ(x:A).λ(xs:List A).λ(C:★).λ(n:C).λ(c:A→C→C).c x (xs C n c)
 */
function defineList() {
  // Church-encoded List A = Π(C:★). C → (A → C → C) → C
  // This is a fold-based encoding (catamorphism).
  const A = new Var('A');
  
  const listType = new Pi('A', new Star(),
    new Pi('C', new Star(),
      arrow(new Var('C'),
        arrow(arrow(A, arrow(new Var('C'), new Var('C'))),
          new Var('C')))));
  
  // nil : Π(A:★). List A
  // nil = λ(A:★).λ(C:★).λ(n:C).λ(c:A→C→C).n
  const nil = new Lam('A', new Star(),
    new Lam('C', new Star(),
      new Lam('n', new Var('C'),
        new Lam('c', arrow(A, arrow(new Var('C'), new Var('C'))),
          new Var('n')))));
  
  // cons : Π(A:★). A → List A → List A
  // cons = λ(A:★).λ(x:A).λ(xs:List A).λ(C:★).λ(n:C).λ(c:A→C→C). c x (xs C n c)
  const listA = new Pi('C', new Star(),
    arrow(new Var('C'),
      arrow(arrow(A, arrow(new Var('C'), new Var('C'))),
        new Var('C'))));
  
  const cons = new Lam('A', new Star(),
    new Lam('x', A,
      new Lam('xs', listA,
        new Lam('C', new Star(),
          new Lam('n', new Var('C'),
            new Lam('c', arrow(A, arrow(new Var('C'), new Var('C'))),
              new App(new App(new Var('c'), new Var('x')),
                new App(new App(new App(new Var('xs'), new Var('C')), new Var('n')), new Var('c')))))))));
  
  return {
    name: 'List',
    params: [{ name: 'A', type: new Star() }],
    type: listType,
    constructors: { nil, cons },
    eliminatorName: 'listElim'
  };
}

/**
 * Pair A B = Π(C:★). (A → B → C) → C
 */
function definePair() {
  return defineInductive('Pair', [
    { name: 'A', type: new Star() },
    { name: 'B', type: new Star() },
  ], [
    { name: 'mkpair', argTypes: (p) => [p.A, p.B] },
  ]);
}

/**
 * Unit = Π(C:★). C → C
 * tt = λ(C:★).λ(x:C).x
 */
function defineUnit() {
  return defineInductive('Unit', [], [
    { name: 'tt', argTypes: () => [] },
  ]);
}

/**
 * Void = Π(C:★). C
 * (no constructors — uninhabited type)
 */
function defineVoid() {
  return defineInductive('Void', [], []);
}

// ============================================================
// Elimination helpers
// ============================================================

// Bool elimination: if b then t else f
function boolElim(b, t, f, resultType) {
  return new App(new App(new App(b, resultType), t), f);
}

// Maybe elimination: maybe n j m
function maybeElim(m, nothing, just, A, resultType) {
  return new App(new App(new App(new App(m, resultType), nothing), just), resultType);
}

// List fold: foldr f z xs
function listFold(xs, z, f, A, resultType) {
  return new App(new App(new App(xs, resultType), z), f);
}

// ============================================================
// Exports
// ============================================================

export {
  defineInductive,
  defineBool, defineMaybe, defineEither, defineList, definePair, defineUnit, defineVoid,
  boolElim, maybeElim, listFold
};
