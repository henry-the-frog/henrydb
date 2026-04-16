import { strict as assert } from 'assert';
import { weaken, contract, exchange, checkWeakening, isLinear, isAffine, isRelevant } from './structural-rules.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('weaken: adds variable', () => {
  const ctx = new Map([['x', 'Int']]);
  assert.ok(weaken(ctx, 'y', 'Bool').has('y'));
});
test('contract: merges', () => {
  const ctx = new Map([['x', 'Int'], ['y', 'Int']]);
  const r = contract(ctx, 'x', 'y');
  assert.ok(!r.has('y'));
  assert.ok(r.has('x'));
});
test('contract: type mismatch → error', () => {
  assert.throws(() => contract(new Map([['x','Int'],['y','Bool']]), 'x', 'y'), /mismatch/);
});
test('exchange: swap', () => {
  const ctx = new Map([['x', 'Int'], ['y', 'Bool']]);
  const r = exchange(ctx, 'x', 'y');
  assert.equal(r.get('x'), 'Bool');
  assert.equal(r.get('y'), 'Int');
});
test('checkWeakening: subset', () => {
  assert.ok(checkWeakening(new Map([['x','Int']]), new Map([['x','Int'],['y','Bool']])));
});
test('checkWeakening: not subset', () => {
  assert.ok(!checkWeakening(new Map([['x','Int']]), new Map([['y','Bool']])));
});
test('isLinear: all 1', () => assert.ok(isLinear({ x: 1, y: 1 })));
test('isLinear: not linear', () => assert.ok(!isLinear({ x: 2 })));
test('isAffine: 0 or 1', () => assert.ok(isAffine({ x: 0, y: 1 })));
test('isRelevant: ≥1', () => assert.ok(isRelevant({ x: 1, y: 2 })));

console.log(`\nStructural rules tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
