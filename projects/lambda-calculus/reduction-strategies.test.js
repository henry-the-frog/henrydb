import { strict as assert } from 'assert';
import { Var, Lam, App, normalReduce, applicativeReduce, isRedex, freeVars, resetFresh } from './reduction-strategies.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { resetFresh(); fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const I = new Lam('x', new Var('x'));
const K = new Lam('x', new Lam('y', new Var('x')));

test('isRedex: (λx.x) y', () => assert.ok(isRedex(new App(I, new Var('y')))));
test('isRedex: x y', () => assert.ok(!isRedex(new App(new Var('x'), new Var('y')))));

test('freeVars: λx.y → {y}', () => {
  const fv = freeVars(new Lam('x', new Var('y')));
  assert.ok(fv.has('y') && !fv.has('x'));
});

test('normal: (λx.x) y → y', () => {
  const r = normalReduce(new App(I, new Var('y')));
  assert.equal(r.result.name, 'y');
  assert.equal(r.steps, 1);
});

test('applicative: (λx.x) y → y', () => {
  const r = applicativeReduce(new App(I, new Var('y')));
  assert.equal(r.result.name, 'y');
});

test('normal: K 1 2 → 1', () => {
  const one = new Var('1');
  const two = new Var('2');
  const r = normalReduce(new App(new App(K, one), two));
  assert.equal(r.result.name, '1');
});

test('both agree on simple terms', () => {
  const expr = new App(I, new Var('a'));
  assert.equal(normalReduce(expr).result.name, applicativeReduce(expr).result.name);
});

test('normal: trace has steps', () => {
  const r = normalReduce(new App(new App(K, new Var('a')), new Var('b')));
  assert.ok(r.trace.length > 1);
});

test('already normal: no steps', () => {
  const r = normalReduce(new Var('x'));
  assert.equal(r.steps, 0);
});

test('lambda body reduces', () => {
  const r = normalReduce(new Lam('z', new App(I, new Var('z'))));
  assert.equal(r.result.body.name, 'z');
});

console.log(`\nReduction strategies tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
