import { strict as assert } from 'assert';
import { TInt, TBool, TFun, infer, check, ENum, EBool, EVar, ELam, EApp, EAnn, EIf } from './bidir-complete.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('infer: num → Int', () => assert.equal(infer(ENum(42), new Map()).tag, 'TInt'));
test('infer: bool → Bool', () => assert.equal(infer(EBool(true), new Map()).tag, 'TBool'));
test('infer: var', () => assert.equal(infer(EVar('x'), new Map([['x', new TInt()]])).tag, 'TInt'));
test('infer: annotated lambda', () => {
  const t = infer(EAnn(ELam('x', EVar('x')), new TFun(new TInt(), new TInt())), new Map());
  assert.equal(t.toString(), '(Int → Int)');
});
test('infer: app', () => {
  const ctx = new Map([['f', new TFun(new TInt(), new TBool())]]);
  assert.equal(infer(EApp(EVar('f'), ENum(42)), ctx).tag, 'TBool');
});
test('check: lambda against function type', () => {
  check(ELam('x', EVar('x')), new TFun(new TInt(), new TInt()), new Map()); // No throw
});
test('check: if', () => {
  check(EIf(EBool(true), ENum(1), ENum(2)), new TInt(), new Map()); // No throw
});
test('check: mismatch → error', () => {
  assert.throws(() => check(ENum(42), new TBool(), new Map()), /Expected/);
});
test('infer: unbound → error', () => {
  assert.throws(() => infer(EVar('x'), new Map()), /Unbound/);
});
test('infer: app non-function → error', () => {
  assert.throws(() => infer(EApp(ENum(42), ENum(1)), new Map()), /Expected function/);
});

console.log(`\nBidirectional (complete) tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
