import { strict as assert } from 'assert';
import {
  TMu, TVar, TSum, TProd, TUnit, tUnit, tInt, tNat, tIntList, tIntTree,
  substitute, unroll, equiEqual,
  fold, unfold,
  zero, succ, natToInt, intToNat,
  nil, cons, listToArray,
  leaf, branch, treeSum
} from './recursive-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Unrolling
test('unroll Nat: μα.(1+α) → (1 + μα.(1+α))', () => {
  const unrolled = unroll(tNat);
  assert.equal(unrolled.tag, 'TSum');
  assert.equal(unrolled.left.tag, 'TUnit');
  assert.equal(unrolled.right.tag, 'TMu'); // recursive occurrence
});

test('unroll List: μα.(1+(Int×α)) → (1 + (Int × μα.(...)))', () => {
  const unrolled = unroll(tIntList);
  assert.equal(unrolled.tag, 'TSum');
  assert.equal(unrolled.right.tag, 'TProd');
});

// Equirecursive equality
test('equiEqual: Nat = Nat', () => {
  assert.ok(equiEqual(tNat, tNat));
});

test('equiEqual: Nat ≠ List', () => {
  assert.ok(!equiEqual(tNat, tIntList));
});

test('equiEqual: unrolled = original (coinductive)', () => {
  assert.ok(equiEqual(unroll(tNat), tNat));
});

// Iso-recursive Nat
test('natToInt: zero → 0', () => assert.equal(natToInt(zero()), 0));
test('natToInt: succ(succ(zero)) → 2', () => assert.equal(natToInt(succ(succ(zero()))), 2));
test('intToNat: 5 → natToInt → 5', () => assert.equal(natToInt(intToNat(5)), 5));

// Iso-recursive List
test('listToArray: nil → []', () => assert.deepStrictEqual(listToArray(nil()), []));
test('listToArray: cons(1, cons(2, nil)) → [1,2]', () => {
  assert.deepStrictEqual(listToArray(cons(1, cons(2, nil()))), [1, 2]);
});

// Iso-recursive Tree
test('treeSum: leaf(5) → 5', () => assert.equal(treeSum(leaf(5)), 5));
test('treeSum: branch(leaf(1), branch(leaf(2), leaf(3))) → 6', () => {
  assert.equal(treeSum(branch(leaf(1), branch(leaf(2), leaf(3)))), 6);
});

// Fold/unfold
test('unfold non-fold → error', () => {
  assert.throws(() => unfold(42), /Expected folded/);
});

test('fold then unfold: roundtrip', () => {
  const val = { tag: 'Left', value: null };
  const folded = fold(tNat, val);
  assert.deepStrictEqual(unfold(folded), val);
});

console.log(`\nRecursive types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
