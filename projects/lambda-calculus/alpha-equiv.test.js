import { strict as assert } from 'assert';
import { Var, Lam, App, Num, alphaEq, toCanonical, canonicalString } from './alpha-equiv.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('αeq: λx.x ≡ λy.y', () => assert.ok(alphaEq(new Lam('x', new Var('x')), new Lam('y', new Var('y')))));
test('αeq: λx.x ≢ λx.y', () => assert.ok(!alphaEq(new Lam('x', new Var('x')), new Lam('x', new Var('y')))));
test('αeq: λx.λy.x ≡ λa.λb.a', () => assert.ok(alphaEq(new Lam('x', new Lam('y', new Var('x'))), new Lam('a', new Lam('b', new Var('a'))))));
test('αeq: numbers', () => assert.ok(alphaEq(new Num(42), new Num(42))));
test('αeq: diff numbers', () => assert.ok(!alphaEq(new Num(1), new Num(2))));
test('αeq: apps', () => assert.ok(alphaEq(new App(new Var('f'), new Var('x')), new App(new Var('f'), new Var('x')))));
test('αeq: nested λ', () => {
  const a = new Lam('x', new Lam('y', new App(new Var('x'), new Var('y'))));
  const b = new Lam('a', new Lam('b', new App(new Var('a'), new Var('b'))));
  assert.ok(alphaEq(a, b));
});
test('αeq: shadowing', () => {
  const a = new Lam('x', new Lam('x', new Var('x')));
  const b = new Lam('y', new Lam('z', new Var('z')));
  assert.ok(alphaEq(a, b));
});
test('canonical: λx.x = λ_0._0', () => {
  const c = canonicalString(new Lam('x', new Var('x')));
  assert.ok(c.includes('_0'));
});
test('canonical: same for α-equivalent', () => {
  assert.equal(canonicalString(new Lam('x', new Var('x'))), canonicalString(new Lam('y', new Var('y'))));
});

console.log(`\nAlpha equivalence tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
