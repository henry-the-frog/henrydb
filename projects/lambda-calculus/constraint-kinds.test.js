import { strict as assert } from 'assert';
import { Star, Constraint, kFun, ConstraintKind, ConstrainedType, hasConstraint, addConstraint, removeConstraint, satisfies } from './constraint-kinds.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Star kind', () => assert.equal(Star.name, '*'));
test('Constraint kind', () => assert.equal(Constraint.name, 'Constraint'));
test('kFun: * → *', () => assert.ok(kFun(Star, Star).name.includes('→')));

test('ConstraintKind: Show a', () => {
  const ck = new ConstraintKind('Show', 'a');
  assert.equal(ck.kind, Constraint);
  assert.equal(ck.toString(), 'Show a');
});

test('ConstrainedType: (Show a) => a', () => {
  const ct = new ConstrainedType([new ConstraintKind('Show', 'a')], 'a');
  assert.ok(ct.toString().includes('Show'));
});

test('hasConstraint: true', () => {
  const ct = new ConstrainedType([new ConstraintKind('Show', 'a')], 'a');
  assert.ok(hasConstraint(ct, 'Show'));
});

test('hasConstraint: false', () => {
  const ct = new ConstrainedType([], 'a');
  assert.ok(!hasConstraint(ct, 'Show'));
});

test('addConstraint', () => {
  const ct = new ConstrainedType([], 'a');
  const ct2 = addConstraint(ct, new ConstraintKind('Eq', 'a'));
  assert.equal(ct2.constraints.length, 1);
});

test('removeConstraint', () => {
  const ct = new ConstrainedType([new ConstraintKind('Show', 'a'), new ConstraintKind('Eq', 'a')], 'a');
  assert.equal(removeConstraint(ct, 'Show').constraints.length, 1);
});

test('satisfies: all available', () => {
  const ct = new ConstrainedType([new ConstraintKind('Show', 'a')], 'a');
  assert.ok(satisfies(ct, new Set(['Show'])));
});

test('satisfies: missing', () => {
  const ct = new ConstrainedType([new ConstraintKind('Show', 'a')], 'a');
  assert.ok(!satisfies(ct, new Set(['Eq'])));
});

console.log(`\nConstraint kinds tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
