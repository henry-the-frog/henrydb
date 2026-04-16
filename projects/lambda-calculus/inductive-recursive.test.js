import { strict as assert } from 'assert';
import { UBool, UNat, UStr, UList, UPair, El, typeCheck, genericEq, genericShow, genericSize } from './inductive-recursive.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('El Bool: type name', () => assert.equal(El(new UBool()).type, 'Bool'));
test('El Nat: type name', () => assert.equal(El(new UNat()).type, 'Nat'));
test('El [Nat]: type name', () => assert.equal(El(new UList(new UNat())).type, '[Nat]'));

test('typeCheck: Bool true → valid', () => {
  assert.ok(typeCheck(new UBool(), true).valid);
});
test('typeCheck: Nat 42 → valid', () => {
  assert.ok(typeCheck(new UNat(), 42).valid);
});
test('typeCheck: Nat -1 → invalid', () => {
  assert.ok(!typeCheck(new UNat(), -1).valid);
});
test('typeCheck: [Nat] [1,2,3] → valid', () => {
  assert.ok(typeCheck(new UList(new UNat()), [1, 2, 3]).valid);
});
test('typeCheck: (Bool × Str) → valid', () => {
  assert.ok(typeCheck(new UPair(new UBool(), new UStr()), [true, 'hi']).valid);
});

test('genericEq: [1,2,3] = [1,2,3]', () => {
  assert.ok(genericEq(new UList(new UNat()), [1, 2, 3], [1, 2, 3]));
});
test('genericEq: [1,2] ≠ [1,3]', () => {
  assert.ok(!genericEq(new UList(new UNat()), [1, 2], [1, 3]));
});

test('genericShow: nested', () => {
  const code = new UPair(new UNat(), new UList(new UStr()));
  assert.equal(genericShow(code, [42, ['a', 'b']]), '(42, ["a", "b"])');
});

test('genericSize: pair of list', () => {
  const code = new UPair(new UNat(), new UList(new UNat()));
  assert.equal(genericSize(code, [1, [2, 3, 4]]), 5); // 1 + (1 + 3)
});

console.log(`\nInductive-recursive tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
