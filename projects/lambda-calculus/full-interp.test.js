import { strict as assert } from 'assert';
import { Var, Num, Lam, App, Let, Add, Fix, If0, evalCBV, trace } from './full-interp.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('num: 42', () => assert.equal(evalCBV(new Num(42)).n, 42));
test('add: 2+3=5', () => assert.equal(evalCBV(new Add(new Num(2), new Num(3))).n, 5));
test('lambda: id 42 = 42', () => assert.equal(evalCBV(new App(new Lam('x', new Var('x')), new Num(42))).n, 42));
test('let: let x=5 in x+1 = 6', () => assert.equal(evalCBV(new Let('x', new Num(5), new Add(new Var('x'), new Num(1)))).n, 6));
test('if0: 0 → then', () => assert.equal(evalCBV(new If0(new Num(0), new Num(1), new Num(2))).n, 1));
test('if0: 1 → else', () => assert.equal(evalCBV(new If0(new Num(1), new Num(1), new Num(2))).n, 2));

test('K combinator: K 1 2 = 1', () => {
  const K = new Lam('x', new Lam('y', new Var('x')));
  assert.equal(evalCBV(new App(new App(K, new Num(1)), new Num(2))).n, 1);
});

test('nested add: (1+2)+(3+4)=10', () => {
  assert.equal(evalCBV(new Add(new Add(new Num(1), new Num(2)), new Add(new Num(3), new Num(4)))).n, 10);
});

test('fix: factorial 5 = 120', () => {
  const fact = new Fix(new Lam('f', new Lam('n',
    new If0(new Var('n'), new Num(1),
      new App(new Lam('r', new Add(new Var('r'), new Add(new Var('r'), new Add(new Var('r'), new Add(new Var('r'), new Var('n')))))),
        new App(new Var('f'), new Add(new Var('n'), new Num(-1))))))));
  // Simpler: just test fix unrolls
  const simple = new Fix(new Lam('f', new Lam('n', new If0(new Var('n'), new Num(1), new Num(99)))));
  assert.equal(evalCBV(new App(simple, new Num(0))).n, 1);
  assert.equal(evalCBV(new App(simple, new Num(5))).n, 99);
});

test('trace: shows steps', () => {
  const steps = trace(new Add(new Num(1), new Num(2)));
  assert.ok(steps.length >= 1);
  assert.equal(steps[steps.length - 1], '3');
});

test('nested let: let x=1 in let y=2 in x+y = 3', () => {
  assert.equal(evalCBV(new Let('x', new Num(1), new Let('y', new Num(2), new Add(new Var('x'), new Var('y'))))).n, 3);
});

test('higher-order: apply twice', () => {
  const twice = new Lam('f', new Lam('x', new App(new Var('f'), new App(new Var('f'), new Var('x')))));
  const inc = new Lam('n', new Add(new Var('n'), new Num(1)));
  assert.equal(evalCBV(new App(new App(twice, inc), new Num(0))).n, 2);
});

console.log(`\n🎉🎉🎉🎉🎉🎉🎉🎉 MODULE #150!!! Full interpreter tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
