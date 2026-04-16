import { strict as assert } from 'assert';
import { Var, Lam, App, Let, Num, Add, If0, normalize, normToString, resetFresh } from './nbe-stlc.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { resetFresh(); fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('identity: (λx.x) → (λx.x)', () => {
  assert.equal(normToString(new Lam('x', new Var('x'))), '(λx0.x0)');
});

test('beta: (λx.x) 5 → 5', () => {
  assert.equal(normToString(new App(new Lam('x', new Var('x')), new Num(5))), '5');
});

test('constant function: (λx.λy.x) 1 2 → 1', () => {
  const K = new Lam('x', new Lam('y', new Var('x')));
  assert.equal(normToString(new App(new App(K, new Num(1)), new Num(2))), '1');
});

test('addition: 3 + 4 → 7', () => {
  assert.equal(normToString(new Add(new Num(3), new Num(4))), '7');
});

test('nested add: (1 + 2) + (3 + 4) → 10', () => {
  assert.equal(normToString(new Add(new Add(new Num(1), new Num(2)), new Add(new Num(3), new Num(4)))), '10');
});

test('let: let x = 5 in x + 1 → 6', () => {
  assert.equal(normToString(new Let('x', new Num(5), new Add(new Var('x'), new Num(1)))), '6');
});

test('if0: if0 0 then 1 else 2 → 1', () => {
  assert.equal(normToString(new If0(new Num(0), new Num(1), new Num(2))), '1');
});

test('if0: if0 1 then 1 else 2 → 2', () => {
  assert.equal(normToString(new If0(new Num(1), new Num(1), new Num(2))), '2');
});

test('partial application: (λx.λy.x+y) 3 → (λy.3+y)', () => {
  const add = new Lam('x', new Lam('y', new Add(new Var('x'), new Var('y'))));
  const partial = new App(add, new Num(3));
  const result = normToString(partial);
  assert.ok(result.includes('3'));
  assert.ok(result.includes('+'));
});

test('free variable preserved', () => {
  const expr = new App(new Lam('x', new Var('y')), new Num(42));
  assert.equal(normToString(expr), 'y');
});

test('eta: λx.f x normalizes with readback', () => {
  const expr = new Lam('x', new App(new Var('f'), new Var('x')));
  const result = normalize(expr);
  assert.equal(result.tag, 'Lam');
});

test('compose: (λf.λg.λx.f(g x)) (+1) (+2) 3 → 6', () => {
  const inc = new Lam('n', new Add(new Var('n'), new Num(1)));
  const add2 = new Lam('n', new Add(new Var('n'), new Num(2)));
  const compose = new Lam('f', new Lam('g', new Lam('x', new App(new Var('f'), new App(new Var('g'), new Var('x'))))));
  const result = normToString(new App(new App(new App(compose, inc), add2), new Num(3)));
  assert.equal(result, '6');
});

console.log(`\nNbE-STLC tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
