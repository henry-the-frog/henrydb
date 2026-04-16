import { strict as assert } from 'assert';
import { Idx, Lam, App, Clos, Cons, Shift, id, shift, step, reduce } from './explicit-subst.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('var[id] = var', () => {
  const r = step(new Clos(new Idx(0), id));
  assert.equal(r.n, 0);
});

test('var[↑] = var+1', () => {
  const r = step(new Clos(new Idx(2), shift));
  assert.equal(r.n, 3);
});

test('0[M·σ] = M', () => {
  const r = step(new Clos(new Idx(0), new Cons(new Idx(42), id)));
  assert.equal(r.n, 42);
});

test('1[M·σ] = 0[σ]', () => {
  const r = step(new Clos(new Idx(1), new Cons(new Idx(42), id)));
  assert.equal(r.tag, 'Clos');
  assert.equal(r.term.n, 0);
});

test('(M N)[σ] distributes', () => {
  const r = step(new Clos(new App(new Idx(0), new Idx(1)), id));
  assert.equal(r.tag, 'App');
  assert.equal(r.fn.tag, 'Clos');
});

test('(λ.M)[σ] extends', () => {
  const r = step(new Clos(new Lam(new Idx(0)), id));
  assert.equal(r.tag, 'Lam');
  assert.equal(r.body.tag, 'Clos');
});

test('beta: (λ.0) M → M', () => {
  const r = reduce(new App(new Lam(new Idx(0)), new Idx(5)));
  assert.equal(r.result.n, 5);
});

test('beta: (λ.0) (λ.0) → λ.0', () => {
  const I = new Lam(new Idx(0));
  const r = reduce(new App(I, I));
  assert.equal(r.result.tag, 'Lam');
});

test('shift by 2', () => {
  const r = step(new Clos(new Idx(3), new Shift(2)));
  assert.equal(r.n, 5);
});

test('reduce steps counted', () => {
  const r = reduce(new App(new Lam(new Idx(0)), new Idx(1)));
  assert.ok(r.steps > 0);
});

console.log(`\nExplicit substitutions tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
