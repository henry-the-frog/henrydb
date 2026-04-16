import { strict as assert } from 'assert';
import { posInt, nat, range, nonEmpty, sorted, subtype, meet, join } from './liquid-types.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('posInt: 5 ok', () => assert.ok(posInt().check(5)));
test('posInt: 0 fail', () => assert.ok(!posInt().check(0)));
test('posInt: -1 fail', () => assert.ok(!posInt().check(-1)));
test('nat: 0 ok', () => assert.ok(nat().check(0)));
test('range: in range', () => assert.ok(range(1, 10).check(5)));
test('range: out of range', () => assert.ok(!range(1, 10).check(11)));
test('nonEmpty: "hello" ok', () => assert.ok(nonEmpty().check('hello')));
test('nonEmpty: "" fail', () => assert.ok(!nonEmpty().check('')));
test('sorted: [1,2,3] ok', () => assert.ok(sorted().check([1, 2, 3])));
test('sorted: [3,1,2] fail', () => assert.ok(!sorted().check([3, 1, 2])));
test('subtype: posInt <: nat', () => assert.ok(subtype(posInt(), nat())));
test('subtype: nat !<: posInt', () => assert.ok(!subtype(nat(), posInt())));
test('meet: posInt ∧ range(1,5)', () => {
  const m = meet(posInt(), range(1, 5));
  assert.ok(m.check(3));
  assert.ok(!m.check(0));
  assert.ok(!m.check(6));
});

console.log(`\nLiquid types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
