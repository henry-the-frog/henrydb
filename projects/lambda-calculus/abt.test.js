import { strict as assert } from 'assert';
import { AVar, lam, app, num, freeVars, subst, alphaEq, wellScoped } from './abt.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('freeVars: var', () => assert.ok(freeVars(new AVar('x')).has('x')));
test('freeVars: lam binds', () => assert.ok(!freeVars(lam('x', new AVar('x'))).has('x')));
test('freeVars: lam with free', () => assert.ok(freeVars(lam('x', new AVar('y'))).has('y')));

test('subst: var', () => assert.equal(subst(new AVar('x'), 'x', new AVar('y')).name, 'y'));
test('subst: bound not replaced', () => {
  const r = subst(lam('x', new AVar('x')), 'x', new AVar('z'));
  assert.ok(alphaEq(r, lam('x', new AVar('x'))));
});

test('alphaEq: λx.x = λy.y', () => assert.ok(alphaEq(lam('x', new AVar('x')), lam('y', new AVar('y')))));
test('alphaEq: λx.x ≠ λx.y', () => assert.ok(!alphaEq(lam('x', new AVar('x')), lam('x', new AVar('y')))));

test('app structure', () => {
  const t = app(lam('x', new AVar('x')), num(42));
  assert.equal(t.name, 'app');
  assert.equal(t.args.length, 2);
});

test('wellScoped: λx.x → true', () => {
  assert.ok(wellScoped(lam('x', new AVar('x'))));
});

test('wellScoped: free var → false', () => {
  assert.ok(!wellScoped(new AVar('x')));
});

console.log(`\nABT tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
