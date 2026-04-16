import { strict as assert } from 'assert';
import { TVar, TFun, TCon, Constraint, Substitution, solveConstraints } from './constraint-solver.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const tInt = new TCon('Int'), tBool = new TCon('Bool');
const a = new TVar('a'), b = new TVar('b');

test('solve: a = Int', () => {
  const r = solveConstraints([new Constraint(a, tInt, 'test')]);
  assert.ok(r.ok);
  assert.equal(r.subst.apply(a).name, 'Int');
});

test('solve: a = Int, b = Bool', () => {
  const r = solveConstraints([new Constraint(a, tInt, 't1'), new Constraint(b, tBool, 't2')]);
  assert.ok(r.ok);
  assert.equal(r.subst.apply(a).name, 'Int');
  assert.equal(r.subst.apply(b).name, 'Bool');
});

test('solve: a→b = Int→Bool', () => {
  const r = solveConstraints([new Constraint(new TFun(a, b), new TFun(tInt, tBool), 'fn')]);
  assert.ok(r.ok);
  assert.equal(r.subst.apply(a).name, 'Int');
  assert.equal(r.subst.apply(b).name, 'Bool');
});

test('solve: Int = Int (trivial)', () => {
  assert.ok(solveConstraints([new Constraint(tInt, tInt, 'same')]).ok);
});

test('solve: Int = Bool → error', () => {
  assert.ok(!solveConstraints([new Constraint(tInt, tBool, 'conflict')]).ok);
});

test('solve: occurs check', () => {
  assert.ok(!solveConstraints([new Constraint(a, new TFun(a, tInt), 'infinite')]).ok);
});

test('substitution: apply', () => {
  const s = new Substitution(new Map([['a', tInt]]));
  assert.equal(s.apply(a).name, 'Int');
});

test('substitution: compose', () => {
  const s1 = new Substitution(new Map([['a', b]]));
  const s2 = new Substitution(new Map([['b', tInt]]));
  assert.equal(s2.compose(s1).apply(a).name, 'Int');
});

test('solve: chain a=b, b=Int → a=Int', () => {
  const r = solveConstraints([new Constraint(a, b, 'c1'), new Constraint(b, tInt, 'c2')]);
  assert.ok(r.ok);
  assert.equal(r.subst.apply(a).name, 'Int');
});

test('solve: empty constraints', () => assert.ok(solveConstraints([]).ok));

console.log(`\nConstraint solver tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
