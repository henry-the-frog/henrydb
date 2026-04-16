/**
 * Normalization Proof Stress Tests
 * 
 * Verify strong normalization for all well-typed STLC terms.
 * Key challenge: deeply nested terms, many beta reductions needed.
 */

import { TBase, TFun, Var, Lam, App, normalize, typecheck, verifyStrongNormalization, isStronglyNormalizing } from './normalization-proof.js';

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); pass++; } catch (e) { fail++; console.log(`FAIL: ${name}\n  ${e.message}`); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

const A = new TBase('A');
const B = new TBase('B');
const AA = new TFun(A, A);
const AB = new TFun(A, B);

console.log('=== Strong Normalization Stress Tests ===');

// ============================================================
// Identity function: λ(x:A).x normalizes to itself
// ============================================================
test('identity normalizes', () => {
  const id = new Lam('x', A, new Var('x'));
  const result = verifyStrongNormalization(id);
  assert(result.theorem, `identity should be SN: ${result.reason}`);
});

// ============================================================
// K combinator: λ(x:A).λ(y:B).x
// ============================================================
test('K combinator normalizes', () => {
  const k = new Lam('x', A, new Lam('y', B, new Var('x')));
  const result = verifyStrongNormalization(k);
  assert(result.theorem, `K should be SN`);
});

// ============================================================
// Application of identity: (λx.x) (λy.y) → λy.y
// ============================================================
test('id applied to id normalizes', () => {
  const id1 = new Lam('x', AA, new Var('x'));
  const id2 = new Lam('y', A, new Var('y'));
  const app = new App(id1, id2);
  const result = verifyStrongNormalization(app);
  assert(result.theorem, `id id should be SN`);
});

// ============================================================
// K applied: (λx.λy.x) a b → a (with free variables)
// ============================================================
test('K applied normalizes to first arg', () => {
  const k = new Lam('x', A, new Lam('y', A, new Var('x')));
  const expr = new App(new App(k, new Var('a')), new Var('b'));
  const env = new Map([['a', A], ['b', A]]);
  const tc = typecheck(expr, env);
  assert(tc.ok, 'K a b should type-check');
  assert(isStronglyNormalizing(expr), 'K a b should be SN');
});

// ============================================================
// Deeply nested: λf.λg.λx. f (g (f (g x)))
// ============================================================
test('deep composition normalizes', () => {
  const inner = new App(new Var('f'),
    new App(new Var('g'),
      new App(new Var('f'),
        new App(new Var('g'), new Var('x')))));
  const term = new Lam('f', AA, new Lam('g', AA, new Lam('x', A, inner)));
  const result = verifyStrongNormalization(term);
  assert(result.theorem, `Deep composition should be SN`);
});

// ============================================================
// Church numeral: λf.λx.f(f(f(f(f x))))  (church 5)
// ============================================================
test('Church numeral 5 normalizes', () => {
  let body = new Var('x');
  for (let i = 0; i < 5; i++) body = new App(new Var('f'), body);
  const church5 = new Lam('f', AA, new Lam('x', A, body));
  const result = verifyStrongNormalization(church5);
  assert(result.theorem, `Church 5 should be SN`);
});

// ============================================================
// Church numeral 10
// ============================================================
test('Church numeral 10 normalizes', () => {
  let body = new Var('x');
  for (let i = 0; i < 10; i++) body = new App(new Var('f'), body);
  const church10 = new Lam('f', AA, new Lam('x', A, body));
  const result = verifyStrongNormalization(church10);
  assert(result.theorem, `Church 10 should be SN`);
});

// ============================================================
// Church addition: succ (church 3) = church 4
// ============================================================
test('Church succ normalizes', () => {
  // succ = λn.λf.λx. f (n f x)
  const nfx = new App(new App(new Var('n'), new Var('f')), new Var('x'));
  const succ = new Lam('n', new TFun(AA, new TFun(A, A)),
    new Lam('f', AA,
      new Lam('x', A, new App(new Var('f'), nfx))));
  const result = verifyStrongNormalization(succ);
  assert(result.theorem, `Church succ should be SN`);
});

// ============================================================
// Flip: λf.λx.λy. f y x — argument reordering
// ============================================================
test('flip normalizes', () => {
  const flip = new Lam('f', new TFun(A, new TFun(B, A)),
    new Lam('x', B,
      new Lam('y', A,
        new App(new App(new Var('f'), new Var('y')), new Var('x')))));
  const result = verifyStrongNormalization(flip);
  assert(result.theorem, `flip should be SN`);
});

// ============================================================
// Multiple beta reductions
// ============================================================
test('multiple beta reductions normalize', () => {
  const id = new Lam('x', A, new Var('x'));
  // id (id (id (id a)))
  let expr = new Var('a');
  for (let i = 0; i < 4; i++) {
    expr = new App(new Lam('x', A, new Var('x')), expr);
  }
  assert(isStronglyNormalizing(expr), '4 nested id applications should normalize');
});

// ============================================================
// Chain of 20 id applications
// ============================================================
test('20 nested id applications normalize', () => {
  let expr = new Var('a');
  for (let i = 0; i < 20; i++) {
    expr = new App(new Lam('x' + i, A, new Var('x' + i)), expr);
  }
  assert(isStronglyNormalizing(expr), '20 nested id applications should normalize');
});

// ============================================================
// Ill-typed terms are rejected
// ============================================================
test('ill-typed term rejected', () => {
  const selfApp = new Lam('x', A, new App(new Var('x'), new Var('x')));
  const tc = typecheck(selfApp);
  assert(!tc.ok, 'Self-application should be ill-typed in STLC');
});

// ============================================================
// Summary
// ============================================================
console.log(`\nNormalization stress tests: ${pass}/${pass + fail} passed`);
if (fail > 0) { console.log(`${fail} FAILED`); process.exit(1); }
