import { strict as assert } from 'assert';
import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, TypeError,
  infer, check, normalize, betaEq, subst, freeVars,
  parse, freshName, resetNames, arrow,
  churchBoolType, churchTrue, churchFalse, identity
} from './coc.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const ctx = new Context();
const star = new Star();
const box = new Box();
const nat = new Nat();
const zero = new Zero();

// ============================================================
// Sorts
// ============================================================

test('★ : □', () => {
  assert.ok(infer(ctx, star) instanceof Box);
});

test('ℕ : ★', () => {
  assert.ok(betaEq(infer(ctx, nat), star));
});

test('0 : ℕ', () => {
  assert.ok(betaEq(infer(ctx, zero), nat));
});

test('S 0 : ℕ', () => {
  assert.ok(betaEq(infer(ctx, new Succ(zero)), nat));
});

test('S (S (S 0)) = 3', () => {
  assert.equal(new Succ(new Succ(new Succ(zero))).toString(), '3');
});

// ============================================================
// Pi types
// ============================================================

test('ℕ → ℕ : ★', () => {
  const piType = arrow(nat, nat);
  assert.ok(betaEq(infer(ctx, piType), star));
});

test('★ → ★ : □', () => {
  const piType = arrow(star, star);
  assert.ok(infer(ctx, piType) instanceof Box);
});

test('Π(A:★).A → A : ★', () => {
  const piType = new Pi('A', star, arrow(new Var('A'), new Var('A')));
  assert.ok(betaEq(infer(ctx, piType), star));
});

test('dependent Pi type Π(n:ℕ).P n : ★ (with P:ℕ→★)', () => {
  const P = new Var('P');
  const depCtx = ctx.extend('P', arrow(nat, star));
  const piType = new Pi('n', nat, new App(P, new Var('n')));
  assert.ok(betaEq(infer(depCtx, piType), star));
});

// ============================================================
// Lambda & Application
// ============================================================

test('identity: λ(A:★).λ(x:A).x : Π(A:★).A → A', () => {
  const id = identity();
  const idType = infer(ctx, id);
  const expected = new Pi('A', star, arrow(new Var('A'), new Var('A')));
  assert.ok(betaEq(idType, expected));
});

test('identity applied: (λ(A:★).λ(x:A).x) ℕ 0 ⟶ 0', () => {
  const id = identity();
  const applied = new App(new App(id, nat), zero);
  assert.ok(betaEq(normalize(applied), zero));
});

test('identity applied to ℕ returns ℕ → ℕ', () => {
  const id = identity();
  const applied = new App(id, nat);
  const resultType = infer(ctx, applied);
  assert.ok(betaEq(resultType, arrow(nat, nat)));
});

test('constant function: λ(A:★).λ(B:★).λ(x:A).λ(y:B).x', () => {
  const k = new Lam('A', star, new Lam('B', star, 
    new Lam('x', new Var('A'), new Lam('y', new Var('B'), new Var('x')))));
  const kType = infer(ctx, k);
  const expected = new Pi('A', star, new Pi('B', star,
    arrow(new Var('A'), arrow(new Var('B'), new Var('A')))));
  assert.ok(betaEq(kType, expected));
});

test('succ function: λ(n:ℕ).S n : ℕ → ℕ', () => {
  const succFn = new Lam('n', nat, new Succ(new Var('n')));
  assert.ok(betaEq(infer(ctx, succFn), arrow(nat, nat)));
});

// ============================================================
// Beta reduction / Normalization
// ============================================================

test('beta: (λ(x:ℕ).x) 0 ⟶ 0', () => {
  const term = new App(new Lam('x', nat, new Var('x')), zero);
  assert.ok(betaEq(term, zero));
});

test('nested beta reduction', () => {
  const term = new App(
    new App(
      new Lam('x', nat, new Lam('y', nat, new Var('x'))),
      new Succ(zero)),
    zero);
  assert.ok(betaEq(term, new Succ(zero)));
});

test('type-level computation: (λ(A:★).A → A) ℕ ⟶ ℕ → ℕ', () => {
  const term = new App(new Lam('A', star, arrow(new Var('A'), new Var('A'))), nat);
  assert.ok(betaEq(term, arrow(nat, nat)));
});

// ============================================================
// Church encodings
// ============================================================

test('Church Bool type', () => {
  const boolType = churchBoolType();
  assert.ok(betaEq(infer(ctx, boolType), star));
});

test('Church true well-typed', () => {
  const t = churchTrue();
  assert.ok(betaEq(infer(ctx, t), churchBoolType()));
});

test('Church false well-typed', () => {
  const f = churchFalse();
  assert.ok(betaEq(infer(ctx, f), churchBoolType()));
});

test('Church if-then-else: true ℕ 1 0 ⟶ 1', () => {
  const t = churchTrue();
  const result = new App(new App(new App(t, nat), new Succ(zero)), zero);
  assert.ok(betaEq(result, new Succ(zero)));
});

test('Church if-then-else: false ℕ 1 0 ⟶ 0', () => {
  const f = churchFalse();
  const result = new App(new App(new App(f, nat), new Succ(zero)), zero);
  assert.ok(betaEq(result, zero));
});

// ============================================================
// NatElim (dependent elimination)
// ============================================================

test('natElim constant: always returns 42', () => {
  // P = λ(n:ℕ).ℕ (motive: always ℕ)
  const P = new Lam('n', nat, nat);
  // z = 42
  const z = new Succ(new Succ(new Succ(zero))); // 3 for simplicity
  // s = λ(k:ℕ).λ(ih:ℕ).ih (just pass through)
  const s = new Lam('k', nat, new Lam('ih', nat, new Var('ih')));
  // natElim on 0 → returns z
  const result0 = normalize(new NatElim(P, z, s, zero));
  assert.ok(betaEq(result0, z));
});

test('natElim addition: plus 2 3 = 5', () => {
  resetNames();
  const P = new Lam('_', nat, nat);
  // plus m n = natElim(λ_.ℕ, m, λk.λih.S ih, n)
  const plus = new Lam('m', nat, new Lam('n', nat,
    new NatElim(P,
      new Var('m'),
      new Lam('k', nat, new Lam('ih', nat, new Succ(new Var('ih')))),
      new Var('n'))));
  
  const two = new Succ(new Succ(zero));
  const three = new Succ(new Succ(new Succ(zero)));
  const five = new Succ(new Succ(new Succ(new Succ(new Succ(zero)))));
  
  const result = normalize(new App(new App(plus, two), three));
  assert.ok(betaEq(result, five), `Expected 5, got ${result}`);
});

test('natElim type-checks addition function', () => {
  resetNames();
  const P = new Lam('_', nat, nat);
  const plus = new Lam('m', nat, new Lam('n', nat,
    new NatElim(P,
      new Var('m'),
      new Lam('k', nat, new Lam('ih', nat, new Succ(new Var('ih')))),
      new Var('n'))));
  
  const plusType = infer(ctx, plus);
  assert.ok(betaEq(plusType, arrow(nat, arrow(nat, nat))));
});

test('natElim multiplication: times 3 2 = 6', () => {
  resetNames();
  const P = new Lam('_', nat, nat);
  // We need plus as a helper
  const plusBody = new Lam('a', nat, new Lam('b', nat,
    new NatElim(P, new Var('a'),
      new Lam('k', nat, new Lam('ih', nat, new Succ(new Var('ih')))),
      new Var('b'))));
  
  // times m n = natElim(λ_.ℕ, 0, λk.λih.plus n ih, m)
  const timesBody = new Lam('m', nat, new Lam('n', nat,
    new NatElim(P, zero,
      new Lam('k', nat, new Lam('ih', nat,
        new App(new App(plusBody, new Var('n')), new Var('ih')))),
      new Var('m'))));
  
  const three = new Succ(new Succ(new Succ(zero)));
  const two = new Succ(new Succ(zero));
  const six = new Succ(new Succ(new Succ(new Succ(new Succ(new Succ(zero))))));
  
  const result = normalize(new App(new App(timesBody, three), two));
  assert.ok(betaEq(result, six), `Expected 6, got ${result}`);
});

// ============================================================
// Substitution & Free Variables
// ============================================================

test('free vars of λ(x:ℕ).x + y', () => {
  const term = new Lam('x', nat, new App(new Var('x'), new Var('y')));
  const fv = freeVars(term);
  assert.ok(fv.has('y'));
  assert.ok(!fv.has('x'));
});

test('substitution avoids capture', () => {
  // λ(y:ℕ).x  [x := y]  should rename binder to avoid capture
  const term = new Lam('y', nat, new Var('x'));
  const result = subst(term, 'x', new Var('y'));
  // Should become λ(y':ℕ).y (not λ(y:ℕ).y which would capture)
  assert.ok(result instanceof Lam);
  assert.ok(result.param !== 'y'); // renamed
});

// ============================================================
// Type errors
// ============================================================

test('type error: unbound variable', () => {
  assert.throws(() => infer(ctx, new Var('x')), /Unbound variable/);
});

test('type error: application of non-function', () => {
  assert.throws(() => infer(ctx, new App(zero, zero)), /Pi type/);
});

test('type error: wrong argument type', () => {
  const fn = new Lam('x', nat, new Var('x'));
  assert.throws(() => infer(ctx, new App(fn, star)), /Type mismatch/);
});

test('type error: Succ of non-Nat', () => {
  const badCtx = ctx.extend('x', star);
  assert.throws(() => infer(badCtx, new Succ(new Var('x'))), /must be ℕ/);
});

// ============================================================
// Parser
// ============================================================

test('parse: ★', () => {
  assert.ok(parse('★') instanceof Star);
  assert.ok(parse('Type') instanceof Star);
});

test('parse: ℕ', () => {
  assert.ok(parse('ℕ') instanceof Nat);
  assert.ok(parse('Nat') instanceof Nat);
});

test('parse: number literal', () => {
  assert.ok(betaEq(parse('3'), new Succ(new Succ(new Succ(zero)))));
});

test('parse: arrow type', () => {
  const t = parse('ℕ → ℕ');
  assert.ok(t instanceof Pi);
  assert.ok(betaEq(t.paramType, nat));
  assert.ok(betaEq(t.body, nat));
});

test('parse: Pi type', () => {
  const t = parse('Π(A:★).A → A');
  assert.ok(t instanceof Pi);
  assert.equal(t.param, 'A');
  assert.ok(t.paramType instanceof Star);
});

test('parse: lambda', () => {
  const t = parse('λ(x:ℕ).x');
  assert.ok(t instanceof Lam);
  assert.equal(t.param, 'x');
  assert.ok(t.paramType instanceof Nat);
  assert.ok(t.body instanceof Var);
});

test('parse + typecheck: identity', () => {
  const id = parse('λ(A:★).λ(x:A).x');
  const idType = infer(ctx, id);
  const expected = parse('Π(A:★).A → A');
  assert.ok(betaEq(idType, expected));
});

test('parse: application', () => {
  const t = parse('f x');
  assert.ok(t instanceof App);
});

test('parse: natElim', () => {
  const t = parse('natElim(λ(n:ℕ).ℕ, 0, λ(k:ℕ).λ(ih:ℕ).S ih, 3)');
  assert.ok(t instanceof NatElim);
  const result = normalize(t);
  assert.ok(betaEq(result, new Succ(new Succ(new Succ(zero)))));
});

// ============================================================
// Alpha equivalence
// ============================================================

test('alpha equivalence: λ(x:ℕ).x = λ(y:ℕ).y', () => {
  const a = new Lam('x', nat, new Var('x'));
  const b = new Lam('y', nat, new Var('y'));
  assert.ok(a.equals(b));
});

test('alpha equivalence: Π(x:ℕ).ℕ = Π(y:ℕ).ℕ', () => {
  const a = new Pi('x', nat, nat);
  const b = new Pi('y', nat, nat);
  assert.ok(a.equals(b));
});

// ============================================================
// Report
// ============================================================

console.log(`\nCoC tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
