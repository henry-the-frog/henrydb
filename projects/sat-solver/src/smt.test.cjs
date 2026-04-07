'use strict';

const {
  BacktrackableUnionFind,
  EUFSolver,
  BoundsSolver,
  SMTSolver,
  parseSmtExpr,
} = require('./smt.cjs');

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

// ============================================================
// Backtrackable Union-Find
// ============================================================

test('UF: basic union and find', () => {
  const uf = new BacktrackableUnionFind();
  assert(!uf.sameClass('a', 'b'));
  uf.union('a', 'b');
  assert(uf.sameClass('a', 'b'));
});

test('UF: transitive closure', () => {
  const uf = new BacktrackableUnionFind();
  uf.union('a', 'b');
  uf.union('b', 'c');
  assert(uf.sameClass('a', 'c'));
});

test('UF: backtrack undoes union', () => {
  const uf = new BacktrackableUnionFind();
  uf.union('a', 'b');
  assert(uf.sameClass('a', 'b'));
  const cp = uf.checkpoint();
  uf.union('b', 'c');
  assert(uf.sameClass('a', 'c'));
  uf.backtrackTo(cp);
  assert(uf.sameClass('a', 'b'));
  assert(!uf.sameClass('a', 'c'));
});

test('UF: multiple backtrack points', () => {
  const uf = new BacktrackableUnionFind();
  const cp1 = uf.checkpoint();
  uf.union('a', 'b');
  const cp2 = uf.checkpoint();
  uf.union('c', 'd');
  const cp3 = uf.checkpoint();
  uf.union('a', 'c');

  assert(uf.sameClass('a', 'd'));
  uf.backtrackTo(cp3);
  assert(!uf.sameClass('a', 'c'));
  assert(uf.sameClass('a', 'b'));
  assert(uf.sameClass('c', 'd'));

  uf.backtrackTo(cp2);
  assert(!uf.sameClass('c', 'd'));
  assert(uf.sameClass('a', 'b'));

  uf.backtrackTo(cp1);
  assert(!uf.sameClass('a', 'b'));
});

test('UF: many elements', () => {
  const uf = new BacktrackableUnionFind();
  for (let i = 0; i < 100; i++) uf.union(i, i + 1);
  assert(uf.sameClass(0, 100));
  assert(!uf.sameClass(0, 200));
});

// ============================================================
// EUF Theory Solver
// ============================================================

test('EUF: simple equality consistent', () => {
  const euf = new EUFSolver();
  euf.addEquality('a', 'b', 1);
  euf.assertTrue(1);
  const r = euf.checkConsistency();
  assert(r.consistent);
});

test('EUF: equality + disequality conflict', () => {
  const euf = new EUFSolver();
  euf.addEquality('a', 'b', 1);
  euf.addEquality('b', 'c', 2);
  euf.addDisequality('a', 'c', 3);
  euf.assertTrue(1);
  euf.assertTrue(2);
  euf.assertTrue(3);
  const r = euf.checkConsistency();
  assert(!r.consistent);
});

test('EUF: disequality without conflict', () => {
  const euf = new EUFSolver();
  euf.addEquality('a', 'b', 1);
  euf.addDisequality('a', 'c', 2);
  euf.assertTrue(1);
  euf.assertTrue(2);
  const r = euf.checkConsistency();
  assert(r.consistent);
});

test('EUF: congruence closure', () => {
  const euf = new EUFSolver();
  // f(a) = x, f(b) = y, a = b → x = y
  euf.addFuncApp('f', ['a'], 'x');
  euf.addFuncApp('f', ['b'], 'y');
  euf.addEquality('a', 'b', 1);
  euf.addDisequality('x', 'y', 2);
  euf.assertTrue(1);   // a = b
  euf.assertTrue(2);   // x ≠ y
  const r = euf.checkConsistency();
  assert(!r.consistent, 'congruence should derive x = y');
});

test('EUF: congruence with different functions', () => {
  const euf = new EUFSolver();
  euf.addFuncApp('f', ['a'], 'x');
  euf.addFuncApp('g', ['a'], 'y');
  euf.addDisequality('x', 'y', 1);
  euf.assertTrue(1);
  const r = euf.checkConsistency();
  assert(r.consistent, 'different functions, no congruence');
});

test('EUF: multi-arg congruence', () => {
  const euf = new EUFSolver();
  // f(a, c) = x, f(b, d) = y
  euf.addFuncApp('f', ['a', 'c'], 'x');
  euf.addFuncApp('f', ['b', 'd'], 'y');
  euf.addEquality('a', 'b', 1);
  euf.addEquality('c', 'd', 2);
  euf.addDisequality('x', 'y', 3);
  euf.assertTrue(1);
  euf.assertTrue(2);
  euf.assertTrue(3);
  const r = euf.checkConsistency();
  assert(!r.consistent, 'multi-arg congruence');
});

test('EUF: backtracking', () => {
  const euf = new EUFSolver();
  euf.addEquality('a', 'b', 1);
  euf.addDisequality('a', 'b', 2);
  euf.push();
  euf.assertTrue(1);  // a = b
  euf.assertTrue(2);  // a ≠ b
  assert(!euf.checkConsistency().consistent);
  euf.pop();
  // After backtrack, should be consistent (no assertions)
  assert(euf.checkConsistency().consistent);
});

test('EUF: assertFalse equality becomes disequality', () => {
  const euf = new EUFSolver();
  euf.addEquality('a', 'b', 1);
  euf.push();
  euf.assertFalse(1);  // ~(a = b) → a ≠ b
  // This alone is fine
  assert(euf.checkConsistency().consistent);

  // But if we also assert a = c and b = c...
  euf.addEquality('a', 'c', 2);
  euf.addEquality('b', 'c', 3);
  euf.assertTrue(2);
  euf.assertTrue(3);
  // Now a = c = b but we said a ≠ b → conflict
  assert(!euf.checkConsistency().consistent);
  euf.pop();
});

// ============================================================
// Bounds Solver (LIA)
// ============================================================

test('Bounds: simple consistent', () => {
  const b = new BoundsSolver();
  b.addAtom('ge', 'x', 0, 1);
  b.addAtom('le', 'x', 10, 2);
  b.assertTrue(1);
  b.assertTrue(2);
  const r = b.checkConsistency();
  assert(r.consistent);
});

test('Bounds: conflict (lower > upper)', () => {
  const b = new BoundsSolver();
  b.addAtom('ge', 'x', 10, 1);
  b.addAtom('le', 'x', 5, 2);
  b.assertTrue(1);
  b.assertTrue(2);
  const r = b.checkConsistency();
  assert(!r.consistent);
});

test('Bounds: equality constrains both', () => {
  const b = new BoundsSolver();
  b.addAtom('eq', 'x', 5, 1);
  b.assertTrue(1);
  const r = b.checkConsistency();
  assert(r.consistent);
  const m = b.getModel();
  eq(m.x, 5);
});

test('Bounds: negation (not x <= 5 → x >= 6)', () => {
  const b = new BoundsSolver();
  b.addAtom('le', 'x', 5, 1);
  b.addAtom('le', 'x', 4, 2);
  b.assertFalse(1);  // ~(x <= 5) → x >= 6
  b.assertTrue(2);   // x <= 4
  // x >= 6 AND x <= 4 → conflict
  const r = b.checkConsistency();
  assert(!r.consistent);
});

test('Bounds: multiple variables', () => {
  const b = new BoundsSolver();
  b.addAtom('ge', 'x', 0, 1);
  b.addAtom('le', 'x', 100, 2);
  b.addAtom('ge', 'y', -50, 3);
  b.addAtom('le', 'y', 50, 4);
  b.assertTrue(1); b.assertTrue(2);
  b.assertTrue(3); b.assertTrue(4);
  assert(b.checkConsistency().consistent);
});

test('Bounds: backtracking', () => {
  const b = new BoundsSolver();
  b.addAtom('ge', 'x', 10, 1);
  b.addAtom('le', 'x', 5, 2);
  b.push();
  b.assertTrue(1);
  b.assertTrue(2);
  assert(!b.checkConsistency().consistent);
  b.pop();
  assert(b.checkConsistency().consistent);
});

// ============================================================
// SMT Solver (integrated)
// ============================================================

test('SMT: simple equality SAT', () => {
  const smt = new SMTSolver();
  smt.assert(['=', 'a', 'b']);
  eq(smt.checkSat(), 'SAT');
});

test('SMT: equality chain consistent', () => {
  const smt = new SMTSolver();
  smt.assert(['=', 'a', 'b']);
  smt.assert(['=', 'b', 'c']);
  eq(smt.checkSat(), 'SAT');
});

test('SMT: equality + disequality conflict', () => {
  const smt = new SMTSolver();
  smt.assert(['=', 'a', 'b']);
  smt.assert(['=', 'b', 'c']);
  smt.assert(['distinct', 'a', 'c']);
  eq(smt.checkSat(), 'UNSAT');
});

test('SMT: bounds consistent', () => {
  const smt = new SMTSolver();
  smt.assert(['>=', 'x', 0]);
  smt.assert(['<=', 'x', 10]);
  eq(smt.checkSat(), 'SAT');
});

test('SMT: bounds conflict', () => {
  const smt = new SMTSolver();
  smt.assert(['>=', 'x', 10]);
  smt.assert(['<=', 'x', 5]);
  eq(smt.checkSat(), 'UNSAT');
});

test('SMT: negation of equality', () => {
  const smt = new SMTSolver();
  smt.assert(['not', ['=', 'a', 'b']]);
  eq(smt.checkSat(), 'SAT');
});

test('SMT: conjunction', () => {
  const smt = new SMTSolver();
  smt.assert(['and', ['=', 'a', 'b'], ['=', 'b', 'c']]);
  smt.assert(['distinct', 'a', 'c']);
  eq(smt.checkSat(), 'UNSAT');
});

test('SMT: transitivity conflict', () => {
  const smt = new SMTSolver();
  smt.assert(['=', 'x', 'y']);
  smt.assert(['=', 'y', 'z']);
  smt.assert(['=', 'z', 'w']);
  smt.assert(['distinct', 'x', 'w']);
  eq(smt.checkSat(), 'UNSAT');
});

// ============================================================
// S-expression Parser
// ============================================================

test('parse simple expression', () => {
  const result = parseSmtExpr('(= a b)');
  assert(Array.isArray(result));
  eq(result.length, 1);
  eq(result[0][0], '=');
  eq(result[0][1], 'a');
  eq(result[0][2], 'b');
});

test('parse nested expression', () => {
  const result = parseSmtExpr('(and (= a b) (distinct c d))');
  eq(result[0][0], 'and');
  eq(result[0][1][0], '=');
  eq(result[0][2][0], 'distinct');
});

test('parse numbers', () => {
  const result = parseSmtExpr('(<= x 42)');
  eq(result[0][0], '<=');
  eq(result[0][2], 42);
});

test('parse multiple expressions', () => {
  const result = parseSmtExpr('(= a b) (= c d)');
  eq(result.length, 2);
});

// ============================================================
// Integrated Simplex / LIA via SMT
// ============================================================

test('SMT+Simplex: linear constraint SAT', () => {
  const smt = new SMTSolver();
  // 2x + 3y <= 10, x + y >= 3  (feasible: x=3, y=0)
  smt.assert(['<=', ['+', ['*', 2, 'x'], ['*', 3, 'y']], 10]);
  smt.assert(['>=', ['+', 'x', 'y'], 3]);
  eq(smt.checkSat(), 'SAT');
});

test('SMT+Simplex: linear constraint UNSAT', () => {
  const smt = new SMTSolver();
  // x + y <= 5, x >= 4, y >= 3  (infeasible: min(x+y) = 7 > 5)
  smt.assert(['<=', ['+', 'x', 'y'], 5]);
  smt.assert(['>=', 'x', 4]);
  smt.assert(['>=', 'y', 3]);
  eq(smt.checkSat(), 'UNSAT');
});

test('SMT+Simplex: 3-variable system', () => {
  const smt = new SMTSolver();
  // x + y + z <= 10, x >= 2, y >= 3, z >= 4 (feasible: sum >= 9 <= 10)
  smt.assert(['<=', ['+', ['+', 'x', 'y'], 'z'], 10]);
  smt.assert(['>=', 'x', 2]);
  smt.assert(['>=', 'y', 3]);
  smt.assert(['>=', 'z', 4]);
  eq(smt.checkSat(), 'SAT');
});

test('SMT+Simplex: 3-variable system UNSAT', () => {
  const smt = new SMTSolver();
  // x + y + z <= 8, x >= 3, y >= 3, z >= 3 (infeasible: sum >= 9 > 8)
  smt.assert(['<=', ['+', ['+', 'x', 'y'], 'z'], 8]);
  smt.assert(['>=', 'x', 3]);
  smt.assert(['>=', 'y', 3]);
  smt.assert(['>=', 'z', 3]);
  eq(smt.checkSat(), 'UNSAT');
});

test('SMT+Simplex: negative coefficients', () => {
  const smt = new SMTSolver();
  // 2x - y <= 4, x >= 3, y >= 1 (2*3 - 1 = 5 > 4 → need y >= 2)
  smt.assert(['<=', ['-', ['*', 2, 'x'], 'y'], 4]);
  smt.assert(['>=', 'x', 3]);
  smt.assert(['>=', 'y', 2]);
  eq(smt.checkSat(), 'SAT');
});

test('SMT+Simplex: mixed EUF + LIA', () => {
  const smt = new SMTSolver();
  // Equality + arithmetic: both must be satisfied
  smt.assert(['=', 'a', 'b']);
  smt.assert(['>=', 'x', 5]);
  smt.assert(['<=', 'x', 10]);
  eq(smt.checkSat(), 'SAT');
});

test('SMT+Simplex: mixed EUF conflict + LIA SAT', () => {
  const smt = new SMTSolver();
  // LIA is feasible but EUF conflicts
  smt.assert(['=', 'a', 'b']);
  smt.assert(['distinct', 'a', 'b']);
  smt.assert(['>=', 'x', 0]);
  eq(smt.checkSat(), 'UNSAT');
});

test('SMT+Simplex: equality as arithmetic', () => {
  const smt = new SMTSolver();
  // x + y = 10, x >= 3, y >= 3 (feasible)
  smt.assert(['=', ['+', 'x', 'y'], 10]);
  smt.assert(['>=', 'x', 3]);
  smt.assert(['>=', 'y', 3]);
  eq(smt.checkSat(), 'SAT');
});

test('SMT+Simplex: equality constraint UNSAT', () => {
  const smt = new SMTSolver();
  // x + y = 5, x >= 4, y >= 4 (infeasible: min sum = 8 ≠ 5)
  smt.assert(['=', ['+', 'x', 'y'], 5]);
  smt.assert(['>=', 'x', 4]);
  smt.assert(['>=', 'y', 4]);
  eq(smt.checkSat(), 'UNSAT');
});

// ============================================================
// Report
// ============================================================

console.log(`\n${passed} passed, ${failed} failed out of ${passed + failed} tests`);
if (failed > 0) process.exit(1);
