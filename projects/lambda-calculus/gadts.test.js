import { strict as assert } from 'assert';
import { ExprGADT, evalExpr, GADTTypeChecker, ExprGADTDef, TApp, TCon, tInt, tBool } from './gadts.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const E = ExprGADT;

// ============================================================
// Evaluation
// ============================================================

test('IntLit(42) → 42', () => assert.equal(evalExpr(E.IntLit(42)), 42));
test('BoolLit(true) → true', () => assert.equal(evalExpr(E.BoolLit(true)), true));
test('Add(3, 4) → 7', () => assert.equal(evalExpr(E.Add(E.IntLit(3), E.IntLit(4))), 7));
test('Eq(3, 3) → true', () => assert.equal(evalExpr(E.Eq(E.IntLit(3), E.IntLit(3))), true));
test('Eq(3, 4) → false', () => assert.equal(evalExpr(E.Eq(E.IntLit(3), E.IntLit(4))), false));

test('If(true, 1, 2) → 1', () => {
  assert.equal(evalExpr(E.If(E.BoolLit(true), E.IntLit(1), E.IntLit(2))), 1);
});

test('If(Eq(2+3, 5), 42, 0) → 42', () => {
  const expr = E.If(E.Eq(E.Add(E.IntLit(2), E.IntLit(3)), E.IntLit(5)), E.IntLit(42), E.IntLit(0));
  assert.equal(evalExpr(expr), 42);
});

test('nested: Add(Add(1,2), Add(3,4)) → 10', () => {
  assert.equal(evalExpr(E.Add(E.Add(E.IntLit(1), E.IntLit(2)), E.Add(E.IntLit(3), E.IntLit(4)))), 10);
});

test('Pair and Fst/Snd', () => {
  const pair = E.Pair(E.IntLit(1), E.BoolLit(true));
  assert.equal(evalExpr(E.Fst(pair)), 1);
  assert.equal(evalExpr(E.Snd(pair)), true);
});

// ============================================================
// Type checking
// ============================================================

test('type check: IntLit returns Expr<Int>', () => {
  const tc = new GADTTypeChecker(ExprGADTDef);
  const result = tc.check({ conName: 'IntLit', args: [42] }, new TApp(new TCon('Expr'), tInt));
  assert.ok(result.ok);
});

test('type check: BoolLit returns Expr<Bool>', () => {
  const tc = new GADTTypeChecker(ExprGADTDef);
  const result = tc.check({ conName: 'BoolLit', args: [true] }, new TApp(new TCon('Expr'), tBool));
  assert.ok(result.ok);
});

test('type check: Add returns Expr<Int>', () => {
  const tc = new GADTTypeChecker(ExprGADTDef);
  const result = tc.check({ conName: 'Add', args: [E.IntLit(1), E.IntLit(2)] }, new TApp(new TCon('Expr'), tInt));
  assert.ok(result.ok);
});

test('type check: IntLit !: Expr<Bool>', () => {
  const tc = new GADTTypeChecker(ExprGADTDef);
  const result = tc.check({ conName: 'IntLit', args: [42] }, new TApp(new TCon('Expr'), tBool));
  assert.ok(!result.ok);
});

// ============================================================
// Report
// ============================================================

console.log(`\nGADT tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
