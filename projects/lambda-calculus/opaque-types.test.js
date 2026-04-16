import { strict as assert } from 'assert';
import { createOpaqueType, PositiveInt, Email, NonEmptyString, Percentage } from './opaque-types.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('create: valid', () => assert.ok(PositiveInt.create(42)));
test('create: invalid → error', () => assert.throws(() => PositiveInt.create(-1), /Invalid/));
test('unwrap: get value', () => assert.equal(PositiveInt.unwrap(PositiveInt.create(42)), 42));
test('unwrap: wrong type → error', () => assert.throws(() => PositiveInt.unwrap({ value: 42 }), /Not a/));
test('is: true for correct type', () => assert.ok(PositiveInt.is(PositiveInt.create(1))));
test('is: false for wrong type', () => assert.ok(!PositiveInt.is(42)));
test('map: transform', () => assert.equal(PositiveInt.unwrap(PositiveInt.map(x => x * 2, PositiveInt.create(21))), 42));
test('Email: valid', () => assert.ok(Email.create('a@b.com')));
test('Email: invalid', () => assert.throws(() => Email.create('no-at'), /Invalid/));
test('NonEmptyString: valid', () => assert.ok(NonEmptyString.create('hello')));
test('NonEmptyString: empty → error', () => assert.throws(() => NonEmptyString.create(''), /Invalid/));
test('Percentage: valid range', () => assert.ok(Percentage.create(50)));
test('Percentage: out of range', () => assert.throws(() => Percentage.create(101), /Invalid/));

console.log(`\nOpaque types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
