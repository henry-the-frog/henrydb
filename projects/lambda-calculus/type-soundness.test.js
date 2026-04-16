import { strict as assert } from 'assert';
import { TInt, TBool, TFun, typecheck, isValue, step, checkProgress, checkPreservation } from './type-soundness.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const N = n => ({ tag:'Num', n }); const B = b => ({ tag:'Bool', b }); const V = n => ({ tag:'Var', name:n });
const Add = (l,r) => ({ tag:'Add', left:l, right:r }); const If = (c,t,f) => ({ tag:'If', cond:c, then:t, else:f });

test('typecheck: num', () => assert.equal(typecheck(N(42)).tag, 'TInt'));
test('typecheck: bool', () => assert.equal(typecheck(B(true)).tag, 'TBool'));
test('typecheck: add', () => assert.equal(typecheck(Add(N(1), N(2))).tag, 'TInt'));
test('typecheck: if', () => assert.equal(typecheck(If(B(true), N(1), N(2))).tag, 'TInt'));
test('typecheck: add mismatch → error', () => assert.throws(() => typecheck(Add(N(1), B(true))), /need Int/));

test('step: 2+3 → 5', () => assert.equal(step(Add(N(2), N(3))).n, 5));
test('step: if true → then', () => assert.equal(step(If(B(true), N(1), N(2))).n, 1));

test('progress: well-typed can step', () => assert.ok(checkProgress(Add(N(2), N(3)), new Map())));
test('preservation: step preserves type', () => assert.ok(checkPreservation(Add(N(2), N(3)), new Map())));
test('preservation: if preserves type', () => assert.ok(checkPreservation(If(B(true), N(1), N(2)), new Map())));

console.log(`\nType soundness tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
