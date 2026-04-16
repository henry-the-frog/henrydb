/**
 * Calculus of Constructions Stress Tests
 * 
 * Tests the most complex type theory implementation with known
 * challenging cases from the dependent type theory literature.
 */

import {
  Star, Box, Var, Lam, Pi, App, Nat, Zero, Succ, NatElim,
  Context, TypeError,
  infer, check, normalize, betaEq, subst, freeVars,
  arrow, identity, churchBoolType, churchTrue, churchFalse
} from './coc.js';

let pass = 0, fail = 0;
function test(name, fn) {
  try {
    fn();
    pass++;
  } catch (e) {
    fail++;
    console.log(`FAIL: ${name}`);
    console.log(`  ${e.message}`);
  }
}
function assert(cond, msg) { if (!cond) throw new Error(msg || 'assertion failed'); }
function assertThrows(fn) {
  try { fn(); throw new Error('Expected error not thrown'); } catch (e) {
    if (e.message === 'Expected error not thrown') throw e;
  }
}

const ctx = new Context();
const star = new Star();
const box = new Box();
const nat = new Nat();
const zero = new Zero();

console.log('=== CoC Stress Tests ===');

// ============================================================
// Sort axiom: ★ : □
// ============================================================
test('Star has type Box', () => {
  assert(infer(ctx, star) instanceof Box, '★ : □');
});

// ============================================================
// Natural numbers
// ============================================================
test('Nat is a type', () => {
  assert(betaEq(infer(ctx, nat), star), 'Nat : ★');
});

test('zero is a Nat', () => {
  assert(betaEq(infer(ctx, zero), nat), '0 : Nat');
});

test('succ zero is a Nat', () => {
  assert(betaEq(infer(ctx, new Succ(zero)), nat), 'S(0) : Nat');
});

test('double succ is a Nat', () => {
  assert(betaEq(infer(ctx, new Succ(new Succ(zero))), nat), 'S(S(0)) : Nat');
});

// ============================================================
// Polymorphic identity: λ(A:★).λ(x:A).x
// ============================================================
test('polymorphic identity type', () => {
  const id = identity(); // Call the factory function
  const idType = infer(ctx, id);
  assert(idType instanceof Pi, 'id should have Pi type');
});

test('polymorphic identity applied to Nat', () => {
  const id = identity();
  const idNat = new App(id, nat);
  const type = infer(ctx, idNat);
  assert(betaEq(type, arrow(nat, nat)), `id Nat should be Nat → Nat, got ${type}`);
});

test('id Nat 0 = 0', () => {
  const id = identity();
  const result = normalize(new App(new App(id, nat), zero));
  assert(result instanceof Zero, `id Nat 0 should normalize to 0, got ${result}`);
});

// ============================================================
// Pi type formation
// ============================================================
test('simple function type A → A', () => {
  const aa = arrow(nat, nat); // Nat → Nat
  const type = infer(ctx, aa);
  assert(betaEq(type, star), 'Nat → Nat should be a type (★)');
});

test('dependent Pi type', () => {
  // Π(A:★).A → A
  const depPi = new Pi('A', star, arrow(new Var('A'), new Var('A')));
  const type = infer(ctx, depPi);
  assert(betaEq(type, star), 'Π(A:★).A→A should be ★');
});

// ============================================================
// Church Booleans
// ============================================================
test('Church Bool type', () => {
  const boolType = churchBoolType();
  assert(betaEq(infer(ctx, boolType), star), 'Church Bool should be ★');
});

test('Church true has Bool type', () => {
  const trueType = infer(ctx, churchTrue());
  assert(betaEq(trueType, churchBoolType()), `true should have Bool type, got ${trueType}`);
});

test('Church false has Bool type', () => {
  const falseType = infer(ctx, churchFalse());
  assert(betaEq(falseType, churchBoolType()), `false should have Bool type, got ${falseType}`);
});

// ============================================================
// Beta reduction
// ============================================================
test('(λ(x:Nat).x) 0 → 0', () => {
  const term = new App(new Lam('x', nat, new Var('x')), zero);
  const result = normalize(term);
  assert(result instanceof Zero, `(λx.x) 0 should be 0, got ${result}`);
});

test('nested beta: K = λ(x:Nat).λ(y:Nat).x', () => {
  const k = new Lam('x', nat, new Lam('y', nat, new Var('x')));
  const one = new Succ(zero);
  const two = new Succ(one);
  const result = normalize(new App(new App(k, one), two));
  // Should be Succ(Zero)
  assert(result instanceof Succ, `K 1 2 should be 1, got ${result}`);
});

// ============================================================
// Beta equivalence
// ============================================================
test('beta equivalence: (λx.x) y ≡β y', () => {
  const lhs = new App(new Lam('x', nat, new Var('x')), new Var('y'));
  const rhs = new Var('y');
  assert(betaEq(normalize(lhs), rhs), 'Should be beta-equivalent');
});

// ============================================================
// Free variables
// ============================================================
test('free variables in lambda', () => {
  const term = new Lam('x', nat, new App(new Var('x'), new Var('y')));
  const fv = freeVars(term);
  assert(!fv.has('x'), 'x should be bound');
  assert(fv.has('y'), 'y should be free');
});

test('free variables in Pi', () => {
  const term = new Pi('x', nat, new Var('x'));
  const fv = freeVars(term);
  assert(!fv.has('x'), 'x should be bound in Pi');
});

// ============================================================
// NatElim (induction principle)
// ============================================================
test('NatElim on zero', () => {
  // natElim P base step 0 → base
  const P = new Lam('n', nat, star); // constant motive
  const base = zero; // base case
  const step = new Lam('k', nat, new Lam('ih', nat, new Succ(new Var('ih')))); // succ case
  
  const result = normalize(new NatElim(P, base, step, zero));
  assert(result instanceof Zero, `natElim on 0 should give base case, got ${result}`);
});

test('NatElim on succ zero', () => {
  const P = new Lam('n', nat, nat);
  const base = zero;
  const step = new Lam('k', nat, new Lam('ih', nat, new Succ(new Var('ih'))));
  
  const result = normalize(new NatElim(P, base, step, new Succ(zero)));
  assert(result instanceof Succ, `natElim on S(0) should give step case, got ${result}`);
});

// ============================================================
// Substitution
// ============================================================
test('substitution in variable', () => {
  const result = subst(new Var('x'), 'x', zero);
  assert(result instanceof Zero, 'x[x:=0] should be 0');
});

test('substitution avoids capture', () => {
  // (λy.x)[x:=y] should NOT capture y
  const term = new Lam('y', nat, new Var('x'));
  const result = subst(term, 'x', new Var('y'));
  // The lambda should rename y to avoid capture
  assert(result instanceof Lam, 'Result should still be a lambda');
  assert(result.name !== 'y' || !freeVars(result.body).has('y'), 
    'Substitution should avoid variable capture');
});

// ============================================================
// Error cases
// ============================================================
test('unbound variable should fail', () => {
  assertThrows(() => infer(ctx, new Var('nonexistent')));
});

test('applying non-function should fail', () => {
  assertThrows(() => infer(ctx, new App(zero, zero)));
});

test('type mismatch should fail', () => {
  // Apply a Nat → Nat function to a Bool type
  const f = new Lam('x', nat, new Var('x'));
  // Apply f to Star (which is not a Nat)
  assertThrows(() => check(ctx, new App(f, star), nat));
});

// ============================================================
// Summary
// ============================================================
console.log(`\nCoC stress tests: ${pass}/${pass + fail} passed`);
if (fail > 0) {
  console.log(`${fail} FAILED`);
  process.exit(1);
}
