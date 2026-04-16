import { strict as assert } from 'assert';
import { sNat, sList, sizeLeq, sizeSucc, sizePred, checkSizedTermination, isSubtype, INFTY } from './sized-types.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('sizeLeq: 3 ≤ 5', () => assert.ok(sizeLeq(3, 5)));
test('sizeLeq: 5 ≤ ∞', () => assert.ok(sizeLeq(5, INFTY)));
test('sizeLeq: ∞ ≤ 5 → false', () => assert.ok(!sizeLeq(INFTY, 5)));
test('sizeSucc: 3 → 4', () => assert.equal(sizeSucc(3), 4));
test('sizePred: 3 → 2', () => assert.equal(sizePred(3), 2));
test('sizePred: 0 → 0', () => assert.equal(sizePred(0), 0));

test('isSubtype: Nat^3 <: Nat^5', () => assert.ok(isSubtype(sNat(3), sNat(5))));
test('isSubtype: Nat^5 !<: Nat^3', () => assert.ok(!isSubtype(sNat(5), sNat(3))));
test('isSubtype: Nat^3 <: Nat^∞', () => assert.ok(isSubtype(sNat(3), sNat(INFTY))));

test('termination: decreasing → ok', () => {
  const r = checkSizedTermination('f', [
    { fn: 'f', paramSizes: [5], argSizes: [4] },
    { fn: 'f', paramSizes: [3], argSizes: [2] }
  ]);
  assert.ok(r.terminates);
});

test('termination: non-decreasing → fail', () => {
  const r = checkSizedTermination('f', [
    { fn: 'f', paramSizes: [3], argSizes: [3] } // Same size!
  ]);
  assert.ok(!r.terminates);
});

console.log(`\nSized types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
