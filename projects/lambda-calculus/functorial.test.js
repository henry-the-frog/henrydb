import { strict as assert } from 'assert';
import { listFunctor, maybeFunctor, constFunctor, idFunctor, checkFunctorLaw1, checkFunctorLaw2, headNat, singletonNat } from './functorial.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('List fmap: double', () => assert.deepStrictEqual(listFunctor.fmap(x => x * 2, [1, 2, 3]), [2, 4, 6]));
test('Maybe fmap: value', () => assert.equal(maybeFunctor.fmap(x => x + 1, 5), 6));
test('Maybe fmap: null', () => assert.equal(maybeFunctor.fmap(x => x + 1, null), null));
test('Const fmap: ignores', () => assert.equal(constFunctor.fmap(x => x * 2, 42), 42));
test('Id fmap: applies', () => assert.equal(idFunctor.fmap(x => x + 1, 41), 42));
test('Functor law 1: List', () => assert.ok(checkFunctorLaw1(listFunctor, [1, 2, 3])));
test('Functor law 2: List', () => assert.ok(checkFunctorLaw2(listFunctor, x => x + 1, x => x * 2, [1, 2])));
test('Functor law 1: Maybe', () => assert.ok(checkFunctorLaw1(maybeFunctor, 42)));
test('head: [1,2,3] → 1', () => assert.equal(headNat.apply([1, 2, 3]), 1));
test('head: [] → null', () => assert.equal(headNat.apply([]), null));
test('singleton: 5 → [5]', () => assert.deepStrictEqual(singletonNat.apply(5), [5]));
test('singleton: null → []', () => assert.deepStrictEqual(singletonNat.apply(null), []));

console.log(`\nFunctorial semantics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
