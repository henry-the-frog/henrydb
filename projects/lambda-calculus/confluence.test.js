import { strict as assert } from 'assert';
import { Var, Lam, App, checkConfluence, parallelReduce, isNormalForm, normalize, leftmostOutermost } from './confluence.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const I = new Lam('x', new Var('x'));
const K = new Lam('x', new Lam('y', new Var('x')));

test('confluence: (λx.x) y', () => assert.ok(checkConfluence(new App(I, new Var('y'))).confluent));
test('confluence: K a b', () => assert.ok(checkConfluence(new App(new App(K, new Var('a')), new Var('b'))).confluent));
test('confluence: (λx.x x)(λx.x)', () => {
  const wI = new App(new Lam('x', new App(new Var('x'), new Var('x'))), I);
  assert.ok(checkConfluence(wI).confluent);
});
test('confluence: nested', () => {
  const expr = new App(new App(K, new App(I, new Var('a'))), new Var('b'));
  assert.ok(checkConfluence(expr).confluent);
});

test('isNormalForm: variable', () => assert.ok(isNormalForm(new Var('x'))));
test('isNormalForm: lambda', () => assert.ok(isNormalForm(new Lam('x', new Var('x')))));
test('isNormalForm: (λx.x) y → false', () => assert.ok(!isNormalForm(new App(I, new Var('y')))));

test('parallel: single step', () => {
  const r = parallelReduce(new App(I, new Var('y')));
  assert.equal(r.name, 'y');
});

test('parallel: multiple redexes at once', () => {
  const expr = new App(new App(I, new Var('f')), new App(I, new Var('x')));
  const r = parallelReduce(expr);
  // Both I applications should be reduced
  assert.equal(r.fn.name, 'f');
  assert.equal(r.arg.name, 'x');
});

test('normalize: reaches NF', () => {
  const r = normalize(new App(new App(K, new Var('a')), new Var('b')), leftmostOutermost);
  assert.equal(r.name, 'a');
});

console.log(`\nConfluence tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
