import { strict as assert } from 'assert';
import { Var, Lam, App, subst, fv, checkSubstLemma, exprEq } from './subst-lemma.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('subst: [y/x]x → y', () => assert.ok(exprEq(subst(new Var('x'), 'x', new Var('y')), new Var('y'))));
test('subst: [y/x]z → z', () => assert.ok(exprEq(subst(new Var('z'), 'x', new Var('y')), new Var('z'))));
test('subst: under lambda', () => {
  const r = subst(new Lam('y', new Var('x')), 'x', new Var('z'));
  assert.ok(exprEq(r.body, new Var('z')));
});
test('subst: bound var not replaced', () => {
  const r = subst(new Lam('x', new Var('x')), 'x', new Var('z'));
  assert.ok(exprEq(r.body, new Var('x')));
});
test('fv: var', () => assert.ok(fv(new Var('x')).has('x')));
test('fv: lam binds', () => assert.ok(!fv(new Lam('x', new Var('x'))).has('x')));
test('fv: app', () => {
  const s = fv(new App(new Var('x'), new Var('y')));
  assert.ok(s.has('x') && s.has('y'));
});
test('substitution lemma: simple case', () => {
  assert.ok(checkSubstLemma(new Var('x'), 'x', new Var('a'), 'y', new Var('b')));
});
test('exprEq: same', () => assert.ok(exprEq(new App(new Var('x'), new Var('y')), new App(new Var('x'), new Var('y')))));
test('exprEq: different', () => assert.ok(!exprEq(new Var('x'), new Var('y'))));

console.log(`\nSubstitution lemma tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
