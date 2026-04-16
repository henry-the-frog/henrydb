import { strict as assert } from 'assert';
import { signDomain, boundsDomain } from './predicate-abstraction.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('sign: positive 5', () => assert.ok(signDomain.abstract(5).positive));
test('sign: negative -3', () => assert.ok(signDomain.abstract(-3).negative));
test('sign: zero 0', () => assert.ok(signDomain.abstract(0).zero));
test('bounds: small 42', () => assert.ok(boundsDomain.abstract(42).small));
test('bounds: large 200', () => assert.ok(boundsDomain.abstract(200).large));
test('join: combines', () => {
  const a = signDomain.abstract(5), b = signDomain.abstract(-3);
  const j = signDomain.join(a, b);
  assert.ok(j.positive); assert.ok(j.negative);
});
test('meet: intersects', () => {
  const a = signDomain.abstract(5), b = signDomain.abstract(-3);
  const m = signDomain.meet(a, b);
  assert.ok(!m.positive); assert.ok(!m.negative);
});
test('implies: subset', () => {
  const pos = signDomain.abstract(5);
  const any = signDomain.join(signDomain.abstract(5), signDomain.abstract(-1));
  assert.ok(signDomain.implies(any, pos));
});
test('isBottom: all false', () => {
  const m = signDomain.meet(signDomain.abstract(5), signDomain.abstract(-3));
  assert.ok(signDomain.isBottom(m));
});
test('refine: set predicate', () => {
  const a = signDomain.abstract(5);
  const r = signDomain.refine(a, 'zero', true);
  assert.ok(r.zero);
});

console.log(`\nPredicate abstraction tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
