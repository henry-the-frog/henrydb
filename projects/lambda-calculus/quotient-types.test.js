import { strict as assert } from 'assert';
import { Rational, ModN, UnorderedPair, SetQ } from './quotient-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Rationals
test('Rational: 1/2 = 2/4', () => {
  assert.ok(Rational.eq(Rational.mk([1, 2]), Rational.mk([2, 4])));
});

test('Rational: 1/2 ≠ 1/3', () => {
  assert.ok(!Rational.eq(Rational.mk([1, 2]), Rational.mk([1, 3])));
});

test('Rational: normalizes 4/6 to 2/3', () => {
  const r = Rational.mk([4, 6]);
  assert.deepStrictEqual(r.value, [2, 3]);
});

test('Rational: negative denominator normalized', () => {
  const r = Rational.mk([1, -2]);
  assert.deepStrictEqual(r.value, [-1, 2]);
});

// Mod N
test('ModN 5: 7 ≡ 2', () => {
  const Z5 = ModN(5);
  assert.ok(Z5.eq(Z5.mk(7), Z5.mk(2)));
});

test('ModN 3: 10 ≡ 1', () => {
  const Z3 = ModN(3);
  assert.ok(Z3.eq(Z3.mk(10), Z3.mk(1)));
});

test('ModN: normalizes', () => {
  const Z5 = ModN(5);
  assert.equal(Z5.mk(7).value, 2);
});

// Unordered pairs
test('unordered pair: (1,2) = (2,1)', () => {
  assert.ok(UnorderedPair.eq(UnorderedPair.mk([1, 2]), UnorderedPair.mk([2, 1])));
});

test('unordered pair: normalizes to sorted', () => {
  assert.deepStrictEqual(UnorderedPair.mk([3, 1]).value, [1, 3]);
});

// Sets
test('set: [1,2,3] = [3,2,1]', () => {
  assert.ok(SetQ.eq(SetQ.mk([1, 2, 3]), SetQ.mk([3, 2, 1])));
});

test('set: [1,1,2] = [1,2]', () => {
  assert.ok(SetQ.eq(SetQ.mk([1, 1, 2]), SetQ.mk([1, 2])));
});

// Equivalence verification
test('Rational: valid equivalence', () => {
  assert.ok(Rational.verifyEquivalence([[1, 2], [2, 4], [3, 6]]).valid);
});

console.log(`\nQuotient types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
