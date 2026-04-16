import { strict as assert } from 'assert';
import { I, K, S, B, C, CombI, CombK, CombS, CombApp, CombVar, combReduce, bracket, combSize } from './combinatory-logic.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// JS-level combinators
test('I 42 = 42', () => assert.equal(I(42), 42));
test('K 1 2 = 1', () => assert.equal(K(1)(2), 1));
test('S K K x = x (I = SKK)', () => assert.equal(S(K)(K)(42), 42));
test('B f g x = f(g(x))', () => assert.equal(B(x => x + 1)(x => x * 2)(3), 7));
test('C f x y = f y x', () => assert.equal(C(x => y => x - y)(1)(10), 9));

// AST-level reduction
test('I x → x', () => {
  const r = combReduce(new CombApp(new CombI(), new CombVar('x')));
  assert.equal(r.result.name, 'x');
});

test('K x y → x', () => {
  const r = combReduce(new CombApp(new CombApp(new CombK(), new CombVar('a')), new CombVar('b')));
  assert.equal(r.result.name, 'a');
});

test('S K K x → x', () => {
  const skk = new CombApp(new CombApp(new CombS(), new CombK()), new CombK());
  const r = combReduce(new CombApp(skk, new CombVar('x')));
  assert.equal(r.result.name, 'x');
});

// Bracket abstraction
test('bracket x in x → I', () => {
  const r = bracket('x', new CombVar('x'));
  assert.equal(r.tag, 'I');
});

test('bracket x in y → K y', () => {
  const r = bracket('x', new CombVar('y'));
  assert.equal(r.fn.tag, 'K');
});

test('combSize: (S (K I)) = 5 (2 apps + 3 atoms)', () => {
  assert.equal(combSize(new CombApp(new CombS(), new CombApp(new CombK(), new CombI()))), 5);
});

console.log(`\nCombinatory logic tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
