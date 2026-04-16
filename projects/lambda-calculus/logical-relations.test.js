import { strict as assert } from 'assert';
import { TBase, TFun, logicalRelation, contextuallyEquivalent, observationallyEquivalent, adequacy, fundamentalTheorem, stepIndexed } from './logical-relations.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const tInt = new TBase('Int');
const tBool = new TBase('Bool');

test('base relation: same ints', () => {
  const r = logicalRelation(tInt, {});
  assert.ok(r(42, 42));
  assert.ok(!r(42, 43));
});

test('base relation: custom', () => {
  const r = logicalRelation(tInt, { Int: (a, b) => a % 2 === b % 2 }); // Same parity
  assert.ok(r(2, 4));
  assert.ok(!r(2, 3));
});

test('function relation: id ∈ R[A→A]', () => {
  const r = logicalRelation(new TFun(tInt, tInt), {});
  const result = r(x => x, x => x);
  assert.ok(result.check(42, 42));
});

test('function relation: different fns, same result', () => {
  const r = logicalRelation(new TFun(tInt, tInt), {});
  const result = r(x => x + 1, x => x + 1);
  assert.ok(result.check(5, 5));
});

test('contextual equivalence', () => {
  const t1 = x => x + 0;
  const t2 = x => x;
  const contexts = [f => f(0), f => f(1), f => f(100)];
  assert.ok(contextuallyEquivalent(t1, t2, contexts));
});

test('contextual non-equivalence', () => {
  const contexts = [f => f(0)];
  assert.ok(!contextuallyEquivalent(x => x, x => x + 1, contexts));
});

test('observational equivalence', () => {
  const obs = [v => typeof v, v => v > 0];
  assert.ok(observationallyEquivalent(5, 10, obs));
});

test('adequacy: related and obs-equiv', () => {
  const r = adequacy((a, b) => a === b, 42, 42, [v => v > 0]);
  assert.ok(r.adequate);
});

test('fundamental theorem: identity', () => {
  const r = fundamentalTheorem(42, tInt, {});
  assert.ok(r); // 42 related to itself
});

test('step-indexed: step 0 always true', () => {
  const r = stepIndexed(0, tInt, {});
  assert.ok(r(1, 999)); // Vacuously true at step 0
});

console.log(`\nLogical relations tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
