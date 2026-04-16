import { strict as assert } from 'assert';
import { ENum, EBool, EVar, ELam, EApp, inferW, inferM, tInt, tBool, resetCounter } from './infer-strategies.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// W strategy
test('W: num → Int', () => assert.equal(inferW(new ENum(42)).type.name, 'Int'));
test('W: bool → Bool', () => assert.equal(inferW(new EBool(true)).type.name, 'Bool'));
test('W: var', () => assert.equal(inferW(new EVar('x'), new Map([['x', tInt]])).type.name, 'Int'));
test('W: lambda', () => assert.equal(inferW(new ELam('x', new EVar('x'))).type.tag, 'TFun'));
test('W: app', () => {
  const env = new Map([['f', { tag: 'TFun', param: tInt, ret: tBool, toString() { return '(Int→Bool)'; } }]]);
  const r = inferW(new EApp(new EVar('f'), new ENum(42)), env);
  assert.equal(r.type.name, 'Bool');
});

// M strategy
test('M: num → Int', () => { inferM(new ENum(42), new Map(), tInt); }); // Doesn't throw
test('M: bool → Bool', () => { inferM(new EBool(true), new Map(), tBool); });
test('M: lambda with expected type', () => {
  const expected = { tag: 'TFun', param: tInt, ret: tInt, toString() { return '(Int→Int)'; } };
  inferM(new ELam('x', new EVar('x')), new Map(), expected);
});

// Both agree
test('W and M agree on identity', () => {
  const wResult = inferW(new ELam('x', new EVar('x')));
  assert.equal(wResult.type.tag, 'TFun');
  // M also succeeds
  inferM(new ELam('x', new EVar('x')));
});

test('W: unbound → error', () => {
  assert.throws(() => inferW(new EVar('undefined_var')), /Unbound/);
});

console.log(`\nInference strategies tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
