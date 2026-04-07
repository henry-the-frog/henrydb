'use strict';

const { Simplex } = require('./simplex.cjs');

let passed = 0, failed = 0;
function test(name, fn) {
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    console.error(`FAIL: ${name}`);
    console.error(`  ${e.message}`);
  }
}
function assert(cond, msg = 'assertion failed') { if (!cond) throw new Error(msg); }
function eq(a, b, msg) { assert(a === b, msg || `expected ${b}, got ${a}`); }
function near(a, b, eps = 1e-6, msg) {
  assert(Math.abs(a - b) < eps, msg || `expected ${b}, got ${a}`);
}

// ============================================================
// Basic Feasibility
// ============================================================

test('single variable with bounds (feasible)', () => {
  const s = new Simplex();
  s.addVar('x');
  s.assertBound('x', '>=', 1);
  s.assertBound('x', '<=', 10);
  const r = s.check();
  assert(r.feasible);
  const m = s.getModel();
  assert(m.x >= 1 && m.x <= 10, `x=${m.x} not in [1,10]`);
});

test('single variable infeasible', () => {
  const s = new Simplex();
  s.addVar('x');
  s.assertBound('x', '>=', 10);
  s.assertBound('x', '<=', 5);
  const r = s.check();
  assert(!r.feasible);
});

test('equality constraint', () => {
  const s = new Simplex();
  s.addVar('x');
  s.assertBound('x', '=', 5);
  const r = s.check();
  assert(r.feasible);
  near(s.getModel().x, 5);
});

// ============================================================
// Linear Constraints
// ============================================================

test('x + y <= 10 (feasible)', () => {
  const s = new Simplex();
  s.addVar('x');
  s.addVar('y');
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}], '<=', 10);
  const r = s.check();
  assert(r.feasible);
  const m = s.getModel();
  assert(m.x + m.y <= 10 + 1e-6, `x+y=${m.x+m.y} > 10`);
});

test('x + y <= 10, x >= 6, y >= 6 (infeasible)', () => {
  const s = new Simplex();
  s.addVar('x');
  s.addVar('y');
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}], '<=', 10);
  s.assertBound('x', '>=', 6);
  s.assertBound('y', '>=', 6);
  const r = s.check();
  assert(!r.feasible);
});

test('2x + 3y <= 12, x >= 0, y >= 0 (feasible)', () => {
  const s = new Simplex();
  s.addVar('x');
  s.addVar('y');
  s.addConstraint([{var:'x',coeff:2}, {var:'y',coeff:3}], '<=', 12);
  s.assertBound('x', '>=', 0);
  s.assertBound('y', '>=', 0);
  const r = s.check();
  assert(r.feasible);
  const m = s.getModel();
  assert(2*m.x + 3*m.y <= 12 + 1e-6);
});

test('multiple constraints', () => {
  const s = new Simplex();
  s.addVar('x');
  s.addVar('y');
  // x + y <= 10
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}], '<=', 10);
  // x - y <= 4
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:-1}], '<=', 4);
  // x >= 0, y >= 0
  s.assertBound('x', '>=', 0);
  s.assertBound('y', '>=', 0);
  const r = s.check();
  assert(r.feasible);
  const m = s.getModel();
  assert(m.x + m.y <= 10 + 1e-6);
  assert(m.x - m.y <= 4 + 1e-6);
});

test('tight constraint system', () => {
  const s = new Simplex();
  s.addVar('x');
  s.addVar('y');
  // x + y = 10
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}], '=', 10);
  // x = 3
  s.assertBound('x', '=', 3);
  const r = s.check();
  assert(r.feasible);
  const m = s.getModel();
  near(m.x, 3);
  near(m.y, 7);
});

test('three variables', () => {
  const s = new Simplex();
  s.addVar('x'); s.addVar('y'); s.addVar('z');
  // x + y + z <= 15
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}, {var:'z',coeff:1}], '<=', 15);
  s.assertBound('x', '>=', 5);
  s.assertBound('y', '>=', 5);
  s.assertBound('z', '>=', 5);
  // 5+5+5 = 15 ≤ 15, so feasible but tight
  const r = s.check();
  assert(r.feasible);
  const m = s.getModel();
  assert(m.x + m.y + m.z <= 15 + 1e-6);
});

test('three variables infeasible', () => {
  const s = new Simplex();
  s.addVar('x'); s.addVar('y'); s.addVar('z');
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}, {var:'z',coeff:1}], '<=', 14);
  s.assertBound('x', '>=', 5);
  s.assertBound('y', '>=', 5);
  s.assertBound('z', '>=', 5);
  // 5+5+5 = 15 > 14
  const r = s.check();
  assert(!r.feasible);
});

// ============================================================
// >= constraints
// ============================================================

test('x + y >= 10 (feasible with bounds)', () => {
  const s = new Simplex();
  s.addVar('x'); s.addVar('y');
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}], '>=', 10);
  s.assertBound('x', '<=', 20);
  s.assertBound('y', '<=', 20);
  const r = s.check();
  assert(r.feasible);
});

test('x + y >= 10, x <= 3, y <= 3 (infeasible)', () => {
  const s = new Simplex();
  s.addVar('x'); s.addVar('y');
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:1}], '>=', 10);
  s.assertBound('x', '<=', 3);
  s.assertBound('y', '<=', 3);
  // max x+y = 6 < 10
  const r = s.check();
  assert(!r.feasible);
});

// ============================================================
// Backtracking
// ============================================================

test('backtrack restores bounds', () => {
  const s = new Simplex();
  s.addVar('x');
  s.assertBound('x', '>=', 0);
  s.assertBound('x', '<=', 10);
  assert(s.check().feasible);

  const cp = s.checkpoint();
  s.assertBound('x', '>=', 20);  // infeasible with x <= 10
  assert(!s.check().feasible);

  s.backtrackTo(cp);
  assert(s.check().feasible);
});

// ============================================================
// Negative coefficients
// ============================================================

test('x - y <= 5', () => {
  const s = new Simplex();
  s.addVar('x'); s.addVar('y');
  s.addConstraint([{var:'x',coeff:1}, {var:'y',coeff:-1}], '<=', 5);
  s.assertBound('x', '=', 10);
  s.assertBound('y', '>=', 0);
  const r = s.check();
  assert(r.feasible);
  const m = s.getModel();
  assert(m.x - m.y <= 5 + 1e-6);
  assert(m.y >= 5 - 1e-6, `y should be >= 5, got ${m.y}`);
});

// ============================================================
// Report
// ============================================================

console.log(`\n${passed} passed, ${failed} failed out of ${passed + failed} tests`);
if (failed > 0) process.exit(1);
