import { strict as assert } from 'assert';
import { TUnique, TShared, TArray, tInt, tStr, UniquenessChecker, UniqueArray, isSubtype } from './uniqueness-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('unique array: create and read', () => {
  const arr = new UniqueArray([1, 2, 3]);
  assert.equal(arr.get(0), 1);
  assert.equal(arr.length, 3);
});

test('unique array: in-place update', () => {
  const arr = new UniqueArray([1, 2, 3]);
  const arr2 = arr.set(1, 99);
  assert.equal(arr2.get(1), 99);
});

test('unique array: old reference consumed after update', () => {
  const arr = new UniqueArray([1, 2, 3]);
  arr.set(1, 99);
  assert.throws(() => arr.get(0), /consume/i);
});

test('unique array: share converts to regular array', () => {
  const arr = new UniqueArray([1, 2, 3]);
  const shared = arr.share();
  assert.deepStrictEqual(shared, [1, 2, 3]);
});

test('unique array: consumed after share', () => {
  const arr = new UniqueArray([1, 2, 3]);
  arr.share();
  assert.throws(() => arr.get(0), /consume/i);
});

test('checker: valid sequence', () => {
  const checker = new UniquenessChecker();
  const result = checker.checkSequence([
    { kind: 'create', var: 'arr' },
    { kind: 'use', var: 'arr', unique: true },
    { kind: 'consume', var: 'arr' },
  ]);
  assert.ok(result.ok);
});

test('checker: use after consume → error', () => {
  const checker = new UniquenessChecker();
  const result = checker.checkSequence([
    { kind: 'consume', var: 'arr' },
    { kind: 'use', var: 'arr', unique: true },
  ]);
  assert.ok(!result.ok);
});

test('checker: double consume → error', () => {
  const checker = new UniquenessChecker();
  const result = checker.checkSequence([
    { kind: 'consume', var: 'arr' },
    { kind: 'consume', var: 'arr' },
  ]);
  assert.ok(!result.ok);
});

test('subtyping: Unique<Int> <: Shared<Int>', () => {
  assert.ok(isSubtype(new TUnique(tInt), new TShared(tInt)));
});

test('subtyping: Shared<Int> !<: Unique<Int>', () => {
  assert.ok(!isSubtype(new TShared(tInt), new TUnique(tInt)));
});

test('chained updates: arr → arr2 → arr3', () => {
  let arr = new UniqueArray([1, 2, 3]);
  arr = arr.set(0, 10);
  arr = arr.set(1, 20);
  arr = arr.set(2, 30);
  assert.deepStrictEqual([arr.get(0), arr.get(1), arr.get(2)], [10, 20, 30]);
});

console.log(`\nUniqueness types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
