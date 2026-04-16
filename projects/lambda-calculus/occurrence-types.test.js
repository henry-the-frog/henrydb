import { strict as assert } from 'assert';
import {
  tInt, tStr, tBool, tNull, TUnion, union,
  typeEquals, narrow, narrowNegate,
  TypeofTest, TruthyTest, NotTest, AndTest,
  narrowEnv, narrowEnvNegate
} from './occurrence-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Typeof narrowing
// ============================================================

test('typeof int: int|str → int', () => {
  const t = union(tInt, tStr);
  const result = narrow(t, new TypeofTest('x', 'number'));
  assert.ok(typeEquals(result, tInt));
});

test('typeof str: int|str → str', () => {
  const t = union(tInt, tStr);
  const result = narrow(t, new TypeofTest('x', 'string'));
  assert.ok(typeEquals(result, tStr));
});

test('typeof negation: int|str NOT int → str', () => {
  const t = union(tInt, tStr);
  const result = narrowNegate(t, new TypeofTest('x', 'number'));
  assert.ok(typeEquals(result, tStr));
});

test('typeof: int|str|bool → int (typeof number)', () => {
  const t = union(tInt, tStr, tBool);
  const result = narrow(t, new TypeofTest('x', 'number'));
  assert.ok(typeEquals(result, tInt));
});

test('typeof negation: int|str|bool NOT int → str|bool', () => {
  const t = union(tInt, tStr, tBool);
  const result = narrowNegate(t, new TypeofTest('x', 'number'));
  assert.ok(result.tag === 'TUnion');
  assert.equal(result.types.length, 2);
});

// ============================================================
// Truthy narrowing
// ============================================================

test('truthy: int|null → int', () => {
  const t = union(tInt, tNull);
  const result = narrow(t, new TruthyTest('x'));
  assert.ok(typeEquals(result, tInt));
});

test('truthy negation: int|null → null', () => {
  const t = union(tInt, tNull);
  const result = narrowNegate(t, new TruthyTest('x'));
  assert.ok(typeEquals(result, tNull));
});

test('truthy: str|null → str', () => {
  const t = union(tStr, tNull);
  const result = narrow(t, new TruthyTest('x'));
  assert.ok(typeEquals(result, tStr));
});

// ============================================================
// Compound tests
// ============================================================

test('not(typeof int): int|str → str', () => {
  const t = union(tInt, tStr);
  const result = narrow(t, new NotTest(new TypeofTest('x', 'number')));
  assert.ok(typeEquals(result, tStr));
});

test('double negation: not(not(typeof int)) = typeof int', () => {
  const t = union(tInt, tStr);
  const result = narrow(t, new NotTest(new NotTest(new TypeofTest('x', 'number'))));
  assert.ok(typeEquals(result, tInt));
});

test('and: typeof number AND truthy', () => {
  const t = union(tInt, tStr, tNull);
  const result = narrow(t, new AndTest(new TypeofTest('x', 'number'), new TruthyTest('x')));
  assert.ok(typeEquals(result, tInt));
});

// ============================================================
// Environment narrowing
// ============================================================

test('env narrow: if typeof x === number', () => {
  const env = new Map([['x', union(tInt, tStr)]]);
  const narrowed = narrowEnv(env, new TypeofTest('x', 'number'));
  assert.ok(typeEquals(narrowed.get('x'), tInt));
});

test('env narrow negate: else branch', () => {
  const env = new Map([['x', union(tInt, tStr)]]);
  const narrowed = narrowEnvNegate(env, new TypeofTest('x', 'number'));
  assert.ok(typeEquals(narrowed.get('x'), tStr));
});

// ============================================================
// Union creation
// ============================================================

test('union deduplicates', () => {
  const t = union(tInt, tInt);
  assert.ok(typeEquals(t, tInt)); // Single type, not union
});

test('union flattens nested', () => {
  const t = union(union(tInt, tStr), tBool);
  assert.equal(t.types.length, 3);
});

// ============================================================
// Report
// ============================================================

console.log(`\nOccurrence typing tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
