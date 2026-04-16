import { strict as assert } from 'assert';
import {
  dpair, fst, snd, mkVec, vecConcat,
  depCond, matchDepCond, depType,
  tInt, tStr
} from './sigma-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Basic dependent pairs
test('dpair: create and project', () => {
  const p = dpair(42, 'hello', null);
  assert.equal(fst(p), 42);
  assert.equal(snd(p), 'hello');
});

test('dpair: with check passes', () => {
  const p = dpair(3, [1, 2, 3], null, (n, arr) => arr.length === n);
  assert.equal(fst(p), 3);
});

test('dpair: with check fails', () => {
  assert.throws(() => dpair(3, [1, 2], null, (n, arr) => arr.length === n), /Type error/);
});

// Length-indexed vectors
test('mkVec: length matches', () => {
  const v = mkVec([10, 20, 30]);
  assert.equal(fst(v), 3);
  assert.equal(snd(v).length, 3);
});

test('mkVec: empty vector', () => {
  const v = mkVec([]);
  assert.equal(fst(v), 0);
});

test('vecConcat: lengths add', () => {
  const v1 = mkVec([1, 2]);
  const v2 = mkVec([3, 4, 5]);
  const v3 = vecConcat(v1, v2);
  assert.equal(fst(v3), 5);
  assert.deepStrictEqual(snd(v3).elements, [1, 2, 3, 4, 5]);
});

// Dependent conditional
test('depCond: true branch', () => {
  const p = depCond(true, 42, 'hello');
  assert.equal(fst(p), true);
  assert.equal(snd(p), 42);
});

test('depCond: false branch', () => {
  const p = depCond(false, 42, 'hello');
  assert.equal(snd(p), 'hello');
});

test('matchDepCond: dispatch on tag', () => {
  const p1 = depCond(true, 42, 'hello');
  const r1 = matchDepCond(p1, n => n * 2, s => s.length);
  assert.equal(r1, 84);

  const p2 = depCond(false, 42, 'hello');
  const r2 = matchDepCond(p2, n => n * 2, s => s.length);
  assert.equal(r2, 5);
});

// Type-level functions
test('depType: compute dependent type', () => {
  const typeFamily = n => n === 0 ? tStr : tInt;
  assert.equal(depType(0, typeFamily).name, 'Str');
  assert.equal(depType(1, typeFamily).name, 'Int');
});

test('vecConcat: empty + non-empty', () => {
  const v1 = mkVec([]);
  const v2 = mkVec([1, 2, 3]);
  const v3 = vecConcat(v1, v2);
  assert.equal(fst(v3), 3);
});

console.log(`\nSigma types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
