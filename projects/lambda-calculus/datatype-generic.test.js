import { strict as assert } from 'assert';
import {
  mkNil, mkCons, listFromArray, listSum, listLength, listToArray,
  mkLeaf, mkBranch, treeSum, treeDepth,
  ana, FSum, FUnit, FProd, FConst, FId
} from './datatype-generic.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Lists
test('listSum: [1,2,3,4,5] → 15', () => {
  assert.equal(listSum(listFromArray([1, 2, 3, 4, 5])), 15);
});

test('listLength: [1,2,3] → 3', () => {
  assert.equal(listLength(listFromArray([1, 2, 3])), 3);
});

test('listToArray: roundtrip', () => {
  assert.deepStrictEqual(listToArray(listFromArray([10, 20, 30])), [10, 20, 30]);
});

test('listSum: empty → 0', () => {
  assert.equal(listSum(mkNil()), 0);
});

test('listLength: empty → 0', () => {
  assert.equal(listLength(mkNil()), 0);
});

// Trees
test('treeSum: leaf(5) → 5', () => {
  assert.equal(treeSum(mkLeaf(5)), 5);
});

test('treeSum: branch(leaf(1), branch(leaf(2), leaf(3))) → 6', () => {
  assert.equal(treeSum(mkBranch(mkLeaf(1), mkBranch(mkLeaf(2), mkLeaf(3)))), 6);
});

test('treeDepth: leaf → 0', () => {
  assert.equal(treeDepth(mkLeaf(1)), 0);
});

test('treeDepth: nested → 2', () => {
  assert.equal(treeDepth(mkBranch(mkLeaf(1), mkBranch(mkLeaf(2), mkLeaf(3)))), 2);
});

// Anamorphism (unfold)
test('ana: generate list [3,2,1]', () => {
  const list = ana(n => {
    if (n <= 0) return new FSum('nil', new FUnit());
    return new FSum('cons', new FProd(new FConst(n), new FId(n - 1)));
  }, 3);
  assert.deepStrictEqual(listToArray(list), [3, 2, 1]);
});

test('cons/nil: manual construction', () => {
  const lst = mkCons(1, mkCons(2, mkNil()));
  assert.deepStrictEqual(listToArray(lst), [1, 2]);
});

console.log(`\nDatatype-generic programming tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
