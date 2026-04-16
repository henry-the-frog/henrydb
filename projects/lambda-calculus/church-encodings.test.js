import { strict as assert } from 'assert';
import {
  cTrue, cFalse, cNot, cAnd, cOr, fromCBool,
  cZero, cSucc, cAdd, cMul, cPred, cIsZero, fromCNat, toCNat,
  cPair, cFst, cSnd,
  cNil, cCons, fromCList, toCList, cHead, cLength, cMap,
  cNothing, cJust, fromCMaybe
} from './church-encodings.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// Booleans
test('true', () => assert.ok(fromCBool(cTrue)));
test('false', () => assert.ok(!fromCBool(cFalse)));
test('not true = false', () => assert.ok(!fromCBool(cNot(cTrue))));
test('and true true', () => assert.ok(fromCBool(cAnd(cTrue)(cTrue))));
test('or false true', () => assert.ok(fromCBool(cOr(cFalse)(cTrue))));

// Naturals
test('zero = 0', () => assert.equal(fromCNat(cZero), 0));
test('succ(zero) = 1', () => assert.equal(fromCNat(cSucc(cZero)), 1));
test('2 + 3 = 5', () => assert.equal(fromCNat(cAdd(toCNat(2))(toCNat(3))), 5));
test('2 × 3 = 6', () => assert.equal(fromCNat(cMul(toCNat(2))(toCNat(3))), 6));
test('pred(3) = 2', () => assert.equal(fromCNat(cPred(toCNat(3))), 2));
test('isZero(0) = true', () => assert.ok(fromCBool(cIsZero(cZero))));
test('isZero(1) = false', () => assert.ok(!fromCBool(cIsZero(cSucc(cZero)))));

// Pairs
test('pair fst', () => assert.equal(cFst(cPair(42)('hello')), 42));
test('pair snd', () => assert.equal(cSnd(cPair(42)('hello')), 'hello'));

// Lists
test('list from/to array', () => assert.deepStrictEqual(fromCList(toCList([1, 2, 3])), [1, 2, 3]));
test('head [1,2,3] = 1', () => assert.equal(cHead(toCList([1, 2, 3])), 1));
test('length [1,2,3] = 3', () => assert.equal(cLength(toCList([1, 2, 3])), 3));
test('map (*2) [1,2,3] = [2,4,6]', () => {
  assert.deepStrictEqual(fromCList(cMap(x => x * 2)(toCList([1, 2, 3]))), [2, 4, 6]);
});

// Maybe
test('nothing', () => assert.equal(fromCMaybe(cNothing).tag, 'Nothing'));
test('just 42', () => assert.equal(fromCMaybe(cJust(42)).value, 42));

console.log(`\nChurch encodings tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
