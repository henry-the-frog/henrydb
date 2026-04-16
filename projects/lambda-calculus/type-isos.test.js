import { strict as assert } from 'assert';
import { Iso, curryIso, prodCommute, prodAssoc, sumCommute, unitProd, distribute, Left, Right } from './type-isos.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('prodCommute: [1,2] ↔ [2,1]', () => {
  assert.ok(prodCommute.verify([[1, 2], [3, 4], ['a', 'b']]).ok);
});

test('prodCommute: roundtrip both directions', () => {
  assert.ok(prodCommute.verifyBoth([[1, 2]], [[3, 4]]).ok);
});

test('prodAssoc: [[1,2],3] ↔ [1,[2,3]]', () => {
  assert.ok(prodAssoc.verify([[[1, 2], 3], [['a', 'b'], 'c']]).ok);
});

test('sumCommute: Left ↔ Right', () => {
  assert.ok(sumCommute.verify([Left(1), Right(2), Left('a')]).ok);
});

test('unitProd: [a,null] ↔ a', () => {
  assert.ok(unitProd.verify([[42, null], ['hello', null]]).ok);
});

test('distribute: [a, Left(b)] ↔ Left([a,b])', () => {
  assert.ok(distribute.verify([
    [1, Left(2)],
    [3, Right(4)],
    ['a', Left('b')],
  ]).ok);
});

test('curry: f(a)(b) ↔ g([a,b])', () => {
  const add = a => b => a + b;
  const curried = curryIso.forward(add);
  assert.equal(curried([3, 4]), 7);
  
  const uncurried = curryIso.backward(curried);
  assert.equal(uncurried(3)(4), 7);
});

test('curry roundtrip on multiplication', () => {
  const mul = a => b => a * b;
  const uncurried = curryIso.forward(mul);
  const recurried = curryIso.backward(uncurried);
  assert.equal(recurried(3)(4), 12);
});

test('Iso: custom verify catches non-isomorphism', () => {
  const bad = new Iso('bad', x => x + 1, x => x); // Not inverse!
  const result = bad.verify([0, 1, 2]);
  assert.ok(!result.ok);
});

test('prodAssoc: backward then forward', () => {
  const input = [1, [2, 3]];
  const rt = prodAssoc.forward(prodAssoc.backward(input));
  assert.deepStrictEqual(rt, input);
});

test('distribute: backward roundtrip', () => {
  const input = Right([5, 10]);
  const rt = distribute.forward(distribute.backward(input));
  assert.deepStrictEqual(rt, input);
});

console.log(`\nType isomorphisms tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
