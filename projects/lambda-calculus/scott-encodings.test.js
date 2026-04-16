import { strict as assert } from 'assert';
import {
  sTrue, sFalse, fromSBool,
  sZero, sSucc, fromSNat, toSNat, sAdd, sIsZero,
  sNil, sCons, fromSList, toSList, sHead, sTail, sLength,
  sNothing, sJust, fromSMaybe, sLeft, sRight, fromSEither
} from './scott-encodings.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('bool: true', () => assert.ok(fromSBool(sTrue)));
test('bool: false', () => assert.ok(!fromSBool(sFalse)));

test('nat: 0', () => assert.equal(fromSNat(sZero), 0));
test('nat: 3', () => assert.equal(fromSNat(toSNat(3)), 3));
test('nat: 2 + 3 = 5', () => assert.equal(fromSNat(sAdd(toSNat(2), toSNat(3))), 5));
test('nat: isZero(0) = true', () => assert.ok(fromSBool(sIsZero(sZero))));
test('nat: isZero(1) = false', () => assert.ok(!fromSBool(sIsZero(sSucc(sZero)))));

test('list: roundtrip', () => assert.deepStrictEqual(fromSList(toSList([1, 2, 3])), [1, 2, 3]));
test('list: head', () => assert.equal(sHead(toSList([10, 20])), 10));
test('list: tail', () => assert.deepStrictEqual(fromSList(sTail(toSList([1, 2, 3]))), [2, 3]));
test('list: length', () => assert.equal(sLength(toSList([1, 2, 3])), 3));

test('maybe: nothing', () => assert.equal(fromSMaybe(sNothing).tag, 'Nothing'));
test('maybe: just', () => assert.equal(fromSMaybe(sJust(42)).value, 42));

test('either: left', () => assert.equal(fromSEither(sLeft('err')).value, 'err'));
test('either: right', () => assert.equal(fromSEither(sRight(42)).value, 42));

console.log(`\nScott encodings tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
