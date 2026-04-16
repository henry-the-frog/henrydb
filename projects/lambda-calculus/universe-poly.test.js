import { strict as assert } from 'assert';
import { Level, Universe, universeOf, piUniverse, cumulativeSubtype, checkConsistency, ConstraintSet } from './universe-poly.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const l0 = new Level(0), l1 = new Level(1), l2 = new Level(2);

test('Level: succ', () => assert.equal(l0.succ().n, 1));
test('Level: max', () => assert.equal(l1.max(l2).n, 2));
test('Level: leq', () => { assert.ok(l0.leq(l1)); assert.ok(!l2.leq(l0)); });

test('Universe: Type₀ : Type₁', () => {
  const u0 = new Universe(l0);
  const u1 = universeOf(u0);
  assert.equal(u1.level.n, 1);
});

test('piUniverse: max(i,j)', () => {
  const u = piUniverse(l1, l2);
  assert.equal(u.level.n, 2);
});

test('cumulativity: Type₀ <: Type₁', () => {
  assert.ok(cumulativeSubtype(new Universe(l0), new Universe(l1)));
  assert.ok(!cumulativeSubtype(new Universe(l2), new Universe(l0)));
});

test('consistency: valid', () => {
  const r = checkConsistency([[new Universe(l0), new Universe(l1)]]);
  assert.ok(r.consistent);
});

test('consistency: Type₁ : Type₁ → inconsistent', () => {
  const r = checkConsistency([[new Universe(l1), new Universe(l1)]]);
  assert.ok(!r.consistent);
});

test('constraints: solvable', () => {
  const cs = new ConstraintSet();
  cs.addLeq(l0, l1);
  cs.addEq(l1, l1);
  assert.ok(cs.solve().solved);
});

test('constraints: unsolvable', () => {
  const cs = new ConstraintSet();
  cs.addLeq(l2, l0);
  assert.ok(!cs.solve().solved);
});

console.log(`\nUniverse polymorphism tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
