import { strict as assert } from 'assert';
import { Instance, CoherenceChecker } from './coherence.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('coherent: no instances', () => assert.ok(new CoherenceChecker().isCoherent()));
test('coherent: distinct types', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Show', 'Int'));
  cc.add(new Instance('Show', 'Bool'));
  assert.ok(cc.isCoherent());
});
test('incoherent: overlapping', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Show', 'Int'));
  cc.add(new Instance('Show', 'Int'));
  assert.ok(!cc.isCoherent());
});
test('incoherent: typevar overlap', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Show', 'Int'));
  cc.add(new Instance('Show', 'a'));
  assert.ok(!cc.isCoherent());
});
test('resolve: found', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Show', 'Int'));
  assert.ok(cc.resolve('Show', 'Int'));
});
test('resolve: not found', () => {
  const cc = new CoherenceChecker();
  assert.equal(cc.resolve('Show', 'Int'), null);
});
test('resolve: priority', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Show', 'a', 0));
  cc.add(new Instance('Show', 'Int', 10));
  const r = cc.resolve('Show', 'Int');
  assert.equal(r.type, 'Int');
});
test('check: returns errors', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Eq', 'Int'));
  cc.add(new Instance('Eq', 'Int'));
  assert.equal(cc.check().length, 1);
});
test('different classes: ok', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Show', 'Int'));
  cc.add(new Instance('Eq', 'Int'));
  assert.ok(cc.isCoherent());
});
test('check error has reason', () => {
  const cc = new CoherenceChecker();
  cc.add(new Instance('Ord', 'Int'));
  cc.add(new Instance('Ord', 'Int'));
  assert.ok(cc.check()[0].reason.includes('Overlapping'));
});

console.log(`\n🎉 Module #185! Coherence tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
