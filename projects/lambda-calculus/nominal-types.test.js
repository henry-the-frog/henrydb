import { strict as assert } from 'assert';
import { NominalType, nominalEqual, structuralEqual, brand, OpaqueType, newtype } from './nominal-types.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const Point = new NominalType('Point2D', { x: 'Int', y: 'Int' });
const Vec = new NominalType('Vector2D', { x: 'Int', y: 'Int' });

test('nominal: same name = equal', () => assert.ok(nominalEqual(Point, Point)));
test('nominal: diff name ≠ equal', () => assert.ok(!nominalEqual(Point, Vec)));
test('structural: same structure = equal', () => assert.ok(structuralEqual(Point, Vec)));

// Brand
const UserId = brand('UserId');
const PostId = brand('PostId');

test('brand: make and check', () => assert.ok(UserId.check(UserId.make(42))));
test('brand: wrong brand fails', () => assert.ok(!PostId.check(UserId.make(42))));
test('brand: unwrap correct', () => assert.equal(UserId.unwrap(UserId.make(42)), 42));
test('brand: unwrap wrong → error', () => assert.throws(() => PostId.unwrap(UserId.make(42)), /Expected PostId/));

// Opaque
const Email = new OpaqueType('Email', 'string', {});
test('opaque: create and unwrap', () => {
  const e = Email.create('test@example.com');
  assert.equal(Email.unwrap(e), 'test@example.com');
});
test('opaque: wrong type → error', () => {
  assert.throws(() => Email.unwrap({ _type: 'URL', _value: 'x' }), /Type mismatch/);
});

// Newtype
const Age = newtype('Age');
test('newtype: wrap/unwrap', () => assert.equal(Age.unwrap(Age.wrap(25)), 25));
test('newtype: is check', () => assert.ok(Age.is(Age.wrap(25))));
test('newtype: is rejects plain', () => assert.ok(!Age.is(25)));

console.log(`\nNominal types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
