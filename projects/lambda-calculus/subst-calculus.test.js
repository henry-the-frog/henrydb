import { strict as assert } from 'assert';
import { Var, Lam, App, Id, Shift, Cons, applySubst, beta } from './subst-calculus.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Id: var unchanged', () => assert.equal(applySubst(new Id(), new Var(0)).idx, 0));
test('Shift: increments', () => assert.equal(applySubst(new Shift(), new Var(0)).idx, 1));
test('Cons: replaces var 0', () => {
  const s = new Cons(new Var(42), new Id());
  assert.equal(applySubst(s, new Var(0)).idx, 42);
});
test('Cons: var 1 → lookup rest', () => {
  const s = new Cons(new Var(99), new Id());
  assert.equal(applySubst(s, new Var(1)).idx, 0);
});
test('App: distributes', () => {
  const r = applySubst(new Id(), new App(new Var(0), new Var(1)));
  assert.equal(r.tag, 'App');
});
test('beta: (λ.0) x → x', () => {
  const r = beta(new Var(0), new Var(42));
  assert.equal(r.idx, 42);
});
test('beta: (λ.1) x → 0', () => {
  const r = beta(new Var(1), new Var(99));
  assert.equal(r.idx, 0);
});
test('Lam: under lambda', () => {
  const r = applySubst(new Id(), new Lam(new Var(0)));
  assert.equal(r.tag, 'Lam');
});
test('beta: (λ.app 0 0) x → app x x', () => {
  const r = beta(new App(new Var(0), new Var(0)), new Var(5));
  assert.equal(r.fn.idx, 5);
  assert.equal(r.arg.idx, 5);
});
test('Shift + Cons: identity on var 0', () => {
  const s = new Cons(new Var(0), new Shift());
  assert.equal(applySubst(s, new Var(0)).idx, 0);
  assert.ok(applySubst(s, new Var(1)).idx >= 1); // Shifted
});

console.log(`\nSubstitution calculus tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
