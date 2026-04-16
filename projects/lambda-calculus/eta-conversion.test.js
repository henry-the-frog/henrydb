import { strict as assert } from 'assert';
import { Var, Lam, App, Num, isEtaReducible, etaReduce, etaExpand, deepEtaReduce, betaEtaEqual, testExtensionality, resetFresh } from './eta-conversion.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { resetFresh(); fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('isEtaReducible: λx.f x → true', () => {
  assert.ok(isEtaReducible(new Lam('x', new App(new Var('f'), new Var('x')))));
});

test('isEtaReducible: λx.f y → false (wrong var)', () => {
  assert.ok(!isEtaReducible(new Lam('x', new App(new Var('f'), new Var('y')))));
});

test('isEtaReducible: λx.x x → false (x free in f=x)', () => {
  assert.ok(!isEtaReducible(new Lam('x', new App(new Var('x'), new Var('x')))));
});

test('etaReduce: λx.f x → f', () => {
  const r = etaReduce(new Lam('x', new App(new Var('f'), new Var('x'))));
  assert.equal(r.name, 'f');
});

test('etaReduce: non-reducible unchanged', () => {
  const expr = new Lam('x', new Var('x'));
  assert.equal(etaReduce(expr), expr);
});

test('etaExpand: f → λη0.f η0', () => {
  const r = etaExpand(new Var('f'));
  assert.equal(r.tag, 'Lam');
  assert.equal(r.body.tag, 'App');
  assert.equal(r.body.fn.name, 'f');
});

test('deepEtaReduce: nested λx.λy.f x y → f', () => {
  const expr = new Lam('x', new Lam('y', new App(new App(new Var('f'), new Var('x')), new Var('y'))));
  const r = deepEtaReduce(expr);
  // Inner: λy.(f x) y → f x
  // Outer: λx.f x → f
  assert.equal(r.name, 'f');
});

test('betaEtaEqual: f = λx.f x', () => {
  const a = new Var('f');
  const b = new Lam('x', new App(new Var('f'), new Var('x')));
  assert.ok(betaEtaEqual(a, b));
});

test('betaEtaEqual: different terms → false', () => {
  assert.ok(!betaEtaEqual(new Var('f'), new Var('g')));
});

test('testExtensionality: (+1) = (x => x+1)', () => {
  assert.ok(testExtensionality(x => x + 1, x => x + 1, [0, 1, 2, 100, -5]));
});

test('testExtensionality: (+1) ≠ (+2)', () => {
  assert.ok(!testExtensionality(x => x + 1, x => x + 2, [0]));
});

console.log(`\n🎉🎉🎉 MODULE #100! Eta conversion tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
