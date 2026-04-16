/**
 * Theorem Proving with CoC + Inductive Types
 * 
 * Proofs as programs, propositions as types.
 * Each test IS a proof — if it type-checks and normalizes correctly,
 * the theorem is proven.
 */

import { strict as assert } from 'assert';
import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, TypeError,
  infer, check, normalize, betaEq, arrow, resetNames, subst
} from './coc.js';

import {
  defineBool, defineMaybe, defineList, definePair, defineUnit,
  boolElim, listFold
} from './inductive.js';

import { eqType, refl } from './coc-proofs.js';

let passed = 0, failed = 0, total = 0;
const ctx = new Context();
const star = new Star();
const nat = new Nat();
const zero = new Zero();
const one = new Succ(zero);
const two = new Succ(one);
const three = new Succ(two);

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const boolDef = defineBool();
const listDef = defineList();
const maybeDef = defineMaybe();

const bTrue = boolDef.constructors.true;
const bFalse = boolDef.constructors.false;

// Bool operations
function boolAnd(a, b) {
  return boolElim(a, b, bFalse, boolDef.type);
}

function boolOr(a, b) {
  return boolElim(a, bTrue, b, boolDef.type);
}

function boolNot(b) {
  return boolElim(b, bFalse, bTrue, boolDef.type);
}

// ============================================================
// Boolean Algebra Laws
// ============================================================

test('Law: NOT (NOT true) = true (double negation)', () => {
  const result = normalize(boolNot(boolNot(bTrue)));
  assert.ok(betaEq(result, bTrue));
});

test('Law: NOT (NOT false) = false (double negation)', () => {
  const result = normalize(boolNot(boolNot(bFalse)));
  assert.ok(betaEq(result, bFalse));
});

test('Law: true AND false = false (conjunction)', () => {
  const result = normalize(boolAnd(bTrue, bFalse));
  assert.ok(betaEq(result, bFalse));
});

test('Law: true AND true = true', () => {
  const result = normalize(boolAnd(bTrue, bTrue));
  assert.ok(betaEq(result, bTrue));
});

test('Law: false OR true = true (disjunction)', () => {
  const result = normalize(boolOr(bFalse, bTrue));
  assert.ok(betaEq(result, bTrue));
});

test('Law: false OR false = false', () => {
  const result = normalize(boolOr(bFalse, bFalse));
  assert.ok(betaEq(result, bFalse));
});

test("De Morgan's Law 1: NOT (a AND b) = (NOT a) OR (NOT b) for all a,b", () => {
  // Verify for all 4 combinations
  for (const a of [bTrue, bFalse]) {
    for (const b of [bTrue, bFalse]) {
      const lhs = normalize(boolNot(boolAnd(a, b)));
      const rhs = normalize(boolOr(boolNot(a), boolNot(b)));
      assert.ok(betaEq(lhs, rhs), `De Morgan 1 failed for a=${a}, b=${b}`);
    }
  }
});

test("De Morgan's Law 2: NOT (a OR b) = (NOT a) AND (NOT b) for all a,b", () => {
  for (const a of [bTrue, bFalse]) {
    for (const b of [bTrue, bFalse]) {
      const lhs = normalize(boolNot(boolOr(a, b)));
      const rhs = normalize(boolAnd(boolNot(a), boolNot(b)));
      assert.ok(betaEq(lhs, rhs), `De Morgan 2 failed for a=${a}, b=${b}`);
    }
  }
});

test('Law: a AND (b OR c) = (a AND b) OR (a AND c) (distributivity)', () => {
  for (const a of [bTrue, bFalse]) {
    for (const b of [bTrue, bFalse]) {
      for (const c of [bTrue, bFalse]) {
        const lhs = normalize(boolAnd(a, boolOr(b, c)));
        const rhs = normalize(boolOr(boolAnd(a, b), boolAnd(a, c)));
        assert.ok(betaEq(lhs, rhs));
      }
    }
  }
});

test('Law: a OR (a AND b) = a (absorption)', () => {
  for (const a of [bTrue, bFalse]) {
    for (const b of [bTrue, bFalse]) {
      const lhs = normalize(boolOr(a, boolAnd(a, b)));
      assert.ok(betaEq(lhs, normalize(a)));
    }
  }
});

// ============================================================
// Natural Number Properties
// ============================================================

function natAdd(m, n) {
  const P = new Lam('_', nat, nat);
  return new NatElim(P, m,
    new Lam('k', nat, new Lam('ih', nat, new Succ(new Var('ih')))),
    n);
}

function natMul(m, n) {
  const P = new Lam('_', nat, nat);
  return new NatElim(P, zero,
    new Lam('k', nat, new Lam('ih', nat, natAdd(n, new Var('ih')))),
    m);
}

test('0 + n = n (additive identity, left)', () => {
  for (const n of [zero, one, two, three]) {
    assert.ok(betaEq(normalize(natAdd(n, zero)), n));
  }
});

test('n + 0 = n (additive identity, right)', () => {
  for (const n of [zero, one, two, three]) {
    assert.ok(betaEq(normalize(natAdd(zero, n)), n));
  }
});

test('Commutativity: 1 + 2 = 2 + 1 = 3', () => {
  const a = normalize(natAdd(one, two));
  const b = normalize(natAdd(two, one));
  assert.ok(betaEq(a, three));
  assert.ok(betaEq(b, three));
});

test('Associativity: (1 + 2) + 3 = 1 + (2 + 3)', () => {
  const lhs = normalize(natAdd(natAdd(one, two), three));
  const rhs = normalize(natAdd(one, natAdd(two, three)));
  assert.ok(betaEq(lhs, rhs));
  const six = new Succ(new Succ(new Succ(three)));
  assert.ok(betaEq(lhs, six));
});

test('Multiplication: 2 * 3 = 6', () => {
  const result = normalize(natMul(two, three));
  const six = new Succ(new Succ(new Succ(new Succ(new Succ(new Succ(zero))))));
  assert.ok(betaEq(result, six));
});

test('Multiplication: 0 * n = 0', () => {
  const result = normalize(natMul(zero, three));
  assert.ok(betaEq(result, zero));
});

test('Distributivity: 2 * (1 + 2) = 2 * 1 + 2 * 2', () => {
  const lhs = normalize(natMul(two, natAdd(one, two)));
  const rhs = normalize(natAdd(natMul(two, one), natMul(two, two)));
  assert.ok(betaEq(lhs, rhs));
});

// ============================================================
// List Properties
// ============================================================

const nil = listDef.constructors.nil;
const cons = listDef.constructors.cons;

function mkList(elems) {
  let list = new App(nil, nat);
  for (let i = elems.length - 1; i >= 0; i--) {
    list = new App(new App(new App(cons, nat), elems[i]), list);
  }
  return list;
}

function listLength(xs) {
  return listFold(xs, zero, new Lam('_', nat, new Lam('acc', nat, new Succ(new Var('acc')))), nat, nat);
}

function listSum(xs) {
  const P = new Lam('_', nat, nat);
  return listFold(xs, zero,
    new Lam('x', nat, new Lam('acc', nat, natAdd(new Var('x'), new Var('acc')))),
    nat, nat);
}

function listAppend(xs, ys) {
  // append xs ys = fold cons ys xs
  // fold: xs C ys (λx.λacc. cons x acc)
  // Actually: xs (List Nat) ys cons_applied
  const listNat = new Pi('C', star, arrow(new Var('C'), arrow(arrow(nat, arrow(new Var('C'), new Var('C'))), new Var('C'))));
  const consFn = new Lam('x', nat, new Lam('acc', listNat,
    new App(new App(new App(cons, nat), new Var('x')), new Var('acc'))));
  return listFold(xs, ys, consFn, nat, listNat);
}

test('List: |[]| = 0', () => {
  assert.ok(betaEq(normalize(listLength(mkList([]))), zero));
});

test('List: |[1, 2, 3]| = 3', () => {
  assert.ok(betaEq(normalize(listLength(mkList([one, two, three]))), three));
});

test('List: sum [1, 2, 3] = 6', () => {
  const six = new Succ(new Succ(new Succ(three)));
  assert.ok(betaEq(normalize(listSum(mkList([one, two, three]))), six));
});

test('List: sum [] = 0', () => {
  assert.ok(betaEq(normalize(listSum(mkList([]))), zero));
});

test('List append: [1] ++ [2, 3] = [1, 2, 3]', () => {
  resetNames();
  const xs = mkList([one]);
  const ys = mkList([two, three]);
  const appended = listAppend(xs, ys);
  
  // Verify by computing length and sum
  const len = normalize(listLength(normalize(appended)));
  assert.ok(betaEq(len, three), `Expected length 3, got ${len}`);
  
  const sum = normalize(listSum(normalize(appended)));
  const six = new Succ(new Succ(new Succ(three)));
  assert.ok(betaEq(sum, six), `Expected sum 6, got ${sum}`);
});

test('List append identity: [] ++ xs = xs (verified by sum)', () => {
  const xs = mkList([one, two, three]);
  const appended = listAppend(mkList([]), xs);
  const originalSum = normalize(listSum(xs));
  const appendedSum = normalize(listSum(normalize(appended)));
  assert.ok(betaEq(originalSum, appendedSum));
});

// ============================================================
// Maybe Functor
// ============================================================

test('Maybe map: map f (just x) = just (f x)', () => {
  resetNames();
  const just = maybeDef.constructors.just;
  const nothing = maybeDef.constructors.nothing;
  
  // just ℕ 5 : Maybe ℕ
  const five = new Succ(new Succ(new Succ(new Succ(new Succ(zero)))));
  const justFive = new App(new App(just, nat), five);
  
  // map succ (just 5) should give just 6
  // map f m = m ℕ nothing (λx. just (f x))
  const succFn = new Lam('n', nat, new Succ(new Var('n')));
  const mapped = new App(new App(new App(justFive, 
    new Pi('C', star, arrow(new Var('C'), arrow(arrow(nat, new Var('C')), new Var('C'))))),
    new App(nothing, nat)),
    new Lam('x', nat, new App(new App(just, nat), new App(succFn, new Var('x')))));
  
  // This is complex because Maybe is Church-encoded. Let me simplify:
  // just 5 = λC.λn.λj. j 5
  // map succ (just 5) = just 5 (Maybe ℕ) nothing (λx. just (succ x))
  //                    = (λx. just (succ x)) 5
  //                    = just (succ 5) = just 6
  
  // Apply directly: just 5 eliminates with the Maybe type
  const maybeNat = normalize(new App(maybeDef.type, nat));
  const result = normalize(
    new App(new App(new App(justFive, maybeNat),
      new App(nothing, nat)),
      new Lam('x', nat, new App(new App(just, nat), new App(succFn, new Var('x'))))));
  
  // Check that result equals just 6
  const six = new Succ(five);
  const justSix = normalize(new App(new App(just, nat), six));
  assert.ok(betaEq(result, justSix), `Expected just 6, got ${result}`);
});

test('Maybe map: map f nothing = nothing', () => {
  resetNames();
  const just = maybeDef.constructors.just;
  const nothing = maybeDef.constructors.nothing;
  const nothingNat = new App(nothing, nat);
  
  const succFn = new Lam('n', nat, new Succ(new Var('n')));
  const maybeNat = normalize(new App(maybeDef.type, nat));
  
  const result = normalize(
    new App(new App(new App(nothingNat, maybeNat),
      new App(nothing, nat)),
      new Lam('x', nat, new App(new App(just, nat), new App(succFn, new Var('x'))))));
  
  assert.ok(betaEq(result, normalize(nothingNat)), `Expected nothing, got ${result}`);
});

// ============================================================
// Equality Proofs
// ============================================================

test('Proof: 1 + 1 = 2', () => {
  const sum = normalize(natAdd(one, one));
  assert.ok(betaEq(sum, two));
  // The Curry-Howard proof: refl(ℕ, 2) proves Eq(ℕ, 1+1, 2)
  // because 1+1 normalizes to 2
  const proof = refl(nat, two);
  const eqTy = eqType(nat, normalize(natAdd(one, one)), two);
  check(ctx, proof, eqTy);
});

test('Proof: 2 + 2 = 4', () => {
  const four = new Succ(new Succ(two));
  const sum = normalize(natAdd(two, two));
  assert.ok(betaEq(sum, four));
  check(ctx, refl(nat, four), eqType(nat, sum, four));
});

test('Proof: 2 * 3 = 3 * 2 (commutativity verified)', () => {
  const lhs = normalize(natMul(two, three));
  const rhs = normalize(natMul(three, two));
  assert.ok(betaEq(lhs, rhs));
  check(ctx, refl(nat, lhs), eqType(nat, lhs, rhs));
});

// ============================================================
// Report
// ============================================================

console.log(`\nTheorem proving tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
