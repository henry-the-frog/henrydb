import { strict as assert } from 'assert';
import { SVar, SNum, SAdd, SIf0, SLam, SSeq, analyzeStrictness, demandType, ABSENT, LAZY, STRICT } from './strictness.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('strict: x + 1 is strict in x', () => {
  assert.ok(analyzeStrictness(new SAdd(new SVar('x'), new SNum(1)), 'x'));
});

test('strict: 1 + 2 is NOT strict in x', () => {
  assert.ok(!analyzeStrictness(new SAdd(new SNum(1), new SNum(2)), 'x'));
});

test('strict: if0 x then 1 else 2 is strict in x', () => {
  assert.ok(analyzeStrictness(new SIf0(new SVar('x'), new SNum(1), new SNum(2)), 'x'));
});

test('strict: if0 0 then x else y → NOT strict in x', () => {
  // x is in a branch, not always evaluated
  assert.ok(!analyzeStrictness(new SIf0(new SNum(0), new SVar('x'), new SVar('y')), 'x'));
});

test('strict: λy.x → NOT strict (body not evaluated)', () => {
  assert.ok(!analyzeStrictness(new SLam('y', new SVar('x')), 'x'));
});

test('strict: seq x y → strict in x', () => {
  assert.ok(analyzeStrictness(new SSeq(new SVar('x'), new SVar('y')), 'x'));
});

// Demand types
test('demand: absent → ABSENT', () => {
  assert.equal(demandType(new SNum(42), 'x'), ABSENT);
});

test('demand: x + 1 → STRICT', () => {
  assert.equal(demandType(new SAdd(new SVar('x'), new SNum(1)), 'x'), STRICT);
});

test('demand: if0 0 then x else 1 → LAZY', () => {
  assert.equal(demandType(new SIf0(new SNum(0), new SVar('x'), new SNum(1)), 'x'), LAZY);
});

test('demand: λy.y → ABSENT for x', () => {
  assert.equal(demandType(new SLam('y', new SVar('y')), 'x'), ABSENT);
});

console.log(`\nStrictness analysis tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
