import { strict as assert } from 'assert';
import { Num, Var, Add, Mul, Lam, App, Let, If0, CallCC, Throw, run } from './cont-semantics.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('num: 42', () => assert.equal(run(new Num(42)), 42));
test('add: 2+3', () => assert.equal(run(new Add(new Num(2), new Num(3))), 5));
test('mul: 4*5', () => assert.equal(run(new Mul(new Num(4), new Num(5))), 20));
test('let: let x = 5 in x + 1', () => assert.equal(run(new Let('x', new Num(5), new Add(new Var('x'), new Num(1)))), 6));
test('lambda: (λx.x+1) 41', () => assert.equal(run(new App(new Lam('x', new Add(new Var('x'), new Num(1))), new Num(41))), 42));
test('if0: 0 → then', () => assert.equal(run(new If0(new Num(0), new Num(1), new Num(2))), 1));
test('if0: 1 → else', () => assert.equal(run(new If0(new Num(1), new Num(1), new Num(2))), 2));
test('nested: (2+3)*(4+5)', () => assert.equal(run(new Mul(new Add(new Num(2), new Num(3)), new Add(new Num(4), new Num(5)))), 45));

test('callcc: no escape → normal result', () => {
  const expr = new CallCC(new Lam('k', new Num(42)));
  assert.equal(run(expr), 42);
});

test('callcc: escape → aborts computation', () => {
  // callcc(λk. 1 + throw(k, 42)) → 42 (not 43!)
  const expr = new Add(new Num(1), new CallCC(new Lam('k', new Add(new Num(100), new Throw(new Var('k'), new Num(42))))));
  assert.equal(run(expr), 43); // 1 + (callcc returns 42)
});

console.log(`\nContinuation semantics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
