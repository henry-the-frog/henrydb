import { strict as assert } from 'assert';
import { ORDERED, LINEAR, AFFINE, RELEVANT, UNRESTRICTED, SubstructuralChecker, isSubMode, joinMode } from './substructural.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('linear: use exactly once → ok', () => {
  const c = new SubstructuralChecker(LINEAR);
  assert.ok(c.check(new Map([['x', 1], ['y', 1]])).ok);
});

test('linear: unused → error', () => {
  const c = new SubstructuralChecker(LINEAR);
  assert.ok(!c.check(new Map([['x', 0]])).ok);
});

test('linear: used twice → error', () => {
  const c = new SubstructuralChecker(LINEAR);
  assert.ok(!c.check(new Map([['x', 2]])).ok);
});

test('affine: unused → ok', () => {
  const c = new SubstructuralChecker(AFFINE);
  assert.ok(c.check(new Map([['x', 0]])).ok);
});

test('affine: used twice → error', () => {
  const c = new SubstructuralChecker(AFFINE);
  assert.ok(!c.check(new Map([['x', 2]])).ok);
});

test('relevant: used twice → ok', () => {
  const c = new SubstructuralChecker(RELEVANT);
  assert.ok(c.check(new Map([['x', 2]])).ok);
});

test('relevant: unused → error', () => {
  const c = new SubstructuralChecker(RELEVANT);
  assert.ok(!c.check(new Map([['x', 0]])).ok);
});

test('unrestricted: anything goes', () => {
  const c = new SubstructuralChecker(UNRESTRICTED);
  assert.ok(c.check(new Map([['x', 0], ['y', 5]])).ok);
});

test('ordered: wrong order → error', () => {
  const c = new SubstructuralChecker(ORDERED);
  assert.ok(!c.checkOrder(['y', 'x'], ['x', 'y']).ok);
});

test('isSubMode: linear <: affine', () => assert.ok(isSubMode(LINEAR, AFFINE)));
test('isSubMode: ordered <: linear', () => assert.ok(isSubMode(ORDERED, LINEAR)));
test('isSubMode: affine !<: linear', () => assert.ok(!isSubMode(AFFINE, LINEAR)));

test('joinMode: linear ∨ affine = affine', () => assert.equal(joinMode(LINEAR, AFFINE), AFFINE));

console.log(`\nSubstructural types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
