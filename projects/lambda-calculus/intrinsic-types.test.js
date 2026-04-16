import { strict as assert } from 'assert';
import { TyNum, TyBool, TyAdd, TyEq, TyIf, TyPair, TyFst, TySnd, eval_ } from './intrinsic-types.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('TyNum: type is Int', () => assert.equal(new TyNum(42).type, 'Int'));
test('TyBool: type is Bool', () => assert.equal(new TyBool(true).type, 'Bool'));
test('TyAdd: Int + Int → Int', () => assert.equal(new TyAdd(new TyNum(2), new TyNum(3)).type, 'Int'));
test('TyAdd: Bool + Int → error', () => assert.throws(() => new TyAdd(new TyBool(true), new TyNum(1)), /need Int/));
test('TyEq: same types ok', () => assert.equal(new TyEq(new TyNum(1), new TyNum(2)).type, 'Bool'));
test('TyEq: diff types → error', () => assert.throws(() => new TyEq(new TyNum(1), new TyBool(true)), /types differ/));
test('TyIf: typed', () => assert.equal(new TyIf(new TyBool(true), new TyNum(1), new TyNum(2)).type, 'Int'));
test('TyIf: non-Bool cond → error', () => assert.throws(() => new TyIf(new TyNum(1), new TyNum(1), new TyNum(2)), /not Bool/));
test('eval: 2+3=5', () => assert.equal(eval_(new TyAdd(new TyNum(2), new TyNum(3))), 5));
test('eval: if true then 1 else 2', () => assert.equal(eval_(new TyIf(new TyBool(true), new TyNum(1), new TyNum(2))), 1));
test('eval: pair fst', () => assert.equal(eval_(new TyFst(new TyPair(new TyNum(1), new TyBool(true)))), 1));
test('eval: eq', () => assert.equal(eval_(new TyEq(new TyNum(5), new TyNum(5))), true));

console.log(`\nIntrinsic types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
