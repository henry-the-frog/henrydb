import { strict as assert } from 'assert';
import { tInt, tBool, TFun, TExpr, cpsTransform, closureConvert, typeCheck, typesEqual, collectFreeVars } from './typed-compile.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('TExpr: num has Int type', () => {
  assert.equal(TExpr.num(42).type.tag, 'TInt');
});

test('TExpr: lam has function type', () => {
  const id = TExpr.lam('x', tInt, TExpr.var('x', tInt));
  assert.equal(id.type.tag, 'TFun');
});

test('TExpr: app type-checks', () => {
  const id = TExpr.lam('x', tInt, TExpr.var('x', tInt));
  const app = TExpr.app(id, TExpr.num(5));
  assert.equal(app.type.tag, 'TInt');
});

test('TExpr: add rejects non-Int', () => {
  assert.throws(() => TExpr.add(TExpr.num(1), TExpr.bool(true)), /non-Int/);
});

test('TExpr: if rejects non-Bool cond', () => {
  assert.throws(() => TExpr.if_(TExpr.num(1), TExpr.num(2), TExpr.num(3)), /non-Bool/);
});

// CPS transform
test('CPS: preserves type', () => {
  const expr = TExpr.add(TExpr.num(1), TExpr.num(2));
  const cps = cpsTransform(expr);
  assert.ok(cps.isCPS);
  assert.equal(cps.type.tag, 'TInt');
});

test('CPS: lambda gets continuation', () => {
  const lam = TExpr.lam('x', tInt, TExpr.var('x', tInt));
  const cps = cpsTransform(lam);
  assert.ok(cps.cont);
});

// Closure conversion
test('closure: captures free vars', () => {
  const body = TExpr.var('y', tInt); // y is free
  const lam = TExpr.lam('x', tInt, body);
  const cc = closureConvert(lam);
  assert.ok(cc.isCC);
  assert.ok(cc.freeVars.includes('y'));
});

// Type checker
test('typeCheck: well-typed → true', () => {
  const expr = TExpr.add(TExpr.num(1), TExpr.num(2));
  assert.ok(typeCheck(expr));
});

test('typeCheck: lambda body', () => {
  const expr = TExpr.lam('x', tInt, TExpr.add(TExpr.var('x', tInt), TExpr.num(1)));
  assert.ok(typeCheck(expr));
});

test('typesEqual: same', () => {
  assert.ok(typesEqual(tInt, tInt));
  assert.ok(typesEqual(new TFun(tInt, tBool), new TFun(tInt, tBool)));
});

test('typesEqual: different', () => {
  assert.ok(!typesEqual(tInt, tBool));
});

console.log(`\nTyped compilation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
