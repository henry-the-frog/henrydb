import { strict as assert } from 'assert';
import { Identity, Maybe, Validation, liftA2, sequenceA, traverse, checkIdentityLaw, checkHomomorphismLaw } from './applicative.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Identity: pure', () => assert.equal(Identity.pure(42).value, 42));
test('Identity: map', () => assert.equal(Identity.pure(21).map(x => x * 2).value, 42));
test('Identity: ap', () => assert.equal(Identity.pure(x => x + 1).ap(Identity.pure(41)).value, 42));

test('Maybe: pure', () => assert.equal(Maybe.pure(42).value, 42));
test('Maybe: nothing + ap → nothing', () => assert.ok(Maybe.nothing().ap(Maybe.pure(1)).isNothing));
test('Maybe: ap success', () => assert.equal(Maybe.pure(x => x * 2).ap(Maybe.pure(21)).value, 42));

test('Validation: accumulate errors', () => {
  const v = Validation.failure(['err1']).ap(Validation.failure(['err2']));
  assert.deepStrictEqual(v.errors, ['err1', 'err2']);
});

test('liftA2: add', () => {
  const r = liftA2((a, b) => a + b, Identity.pure(20), Identity.pure(22));
  assert.equal(r.value, 42);
});

test('sequenceA: [Just 1, Just 2] → Just [1,2]', () => {
  const r = sequenceA([Maybe.pure(1), Maybe.pure(2)], Maybe);
  assert.deepStrictEqual(r.value, [1, 2]);
});

test('sequenceA: [Just 1, Nothing] → Nothing', () => {
  assert.ok(sequenceA([Maybe.pure(1), Maybe.nothing()], Maybe).isNothing);
});

test('identity law', () => assert.ok(checkIdentityLaw(Identity, Identity.pure(42))));
test('homomorphism law', () => assert.ok(checkHomomorphismLaw(Identity, x => x + 1, 41)));

console.log(`\nApplicative tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
