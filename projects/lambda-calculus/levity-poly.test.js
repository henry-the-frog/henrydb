import { strict as assert } from 'assert';
import { RuntimeRep, LevityType, tInt, tIntHash, tDouble, tDoubleHash, tBool, canBeLevityPolymorphic, checkLevity, box, unbox, callingConvention } from './levity-poly.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Int: lifted', () => assert.ok(tInt.isLifted()));
test('Int#: unlifted', () => assert.ok(tIntHash.isUnlifted()));
test('Double: lifted', () => assert.ok(tDouble.isLifted()));
test('Double#: unlifted', () => assert.ok(tDoubleHash.isUnlifted()));

test('canBeLevityPolymorphic: Int → true', () => assert.ok(canBeLevityPolymorphic(tInt)));
test('canBeLevityPolymorphic: Int# → false', () => assert.ok(!canBeLevityPolymorphic(tIntHash)));

test('checkLevity: matching rep', () => assert.ok(checkLevity(RuntimeRep.IntRep, tIntHash)));
test('checkLevity: any accepts all', () => assert.ok(checkLevity('any', tIntHash)));
test('checkLevity: mismatch', () => assert.ok(!checkLevity(RuntimeRep.IntRep, tDouble)));

test('box/unbox roundtrip', () => {
  const boxed = box(42, RuntimeRep.IntRep);
  assert.ok(boxed.boxed);
  const unboxed = unbox(boxed);
  assert.ok(!unboxed.boxed);
  assert.equal(unboxed.value, 42);
});

test('callingConvention: all lifted → wrapper', () => {
  assert.equal(callingConvention([tInt, tBool]), 'wrapper');
});

test('callingConvention: has unlifted → worker', () => {
  assert.equal(callingConvention([tInt, tIntHash]), 'worker');
});

console.log(`\nLevity polymorphism tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
