import { strict as assert } from 'assert';
import {
  TVar, TForall, TFun, TBase, TProd, tInt, tStr, tBool,
  rank, classifyRank, canInfer, containsForall,
  applyBoth, runST, withFile
} from './higher-rank.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Rank classification
test('rank 0: Int → Int', () => {
  assert.equal(rank(new TFun(tInt, tInt)), 0);
});

test('rank 1: ∀a. a → a', () => {
  assert.equal(rank(new TForall('a', new TFun(new TVar('a'), new TVar('a')))), 1);
});

test('rank 2: (∀a. a → a) → Int', () => {
  const polyArg = new TForall('a', new TFun(new TVar('a'), new TVar('a')));
  assert.equal(rank(new TFun(polyArg, tInt)), 2);
});

test('rank 2: (∀a. a → a) → (Int, String)', () => {
  const polyArg = new TForall('a', new TFun(new TVar('a'), new TVar('a')));
  const result = new TProd(tInt, tStr);
  assert.equal(rank(new TFun(polyArg, result)), 2);
});

test('classifyRank: monomorphic', () => {
  assert.equal(classifyRank(tInt), 'monomorphic');
});

test('classifyRank: rank-1', () => {
  assert.ok(classifyRank(new TForall('a', new TVar('a'))).includes('rank-1'));
});

// Contains forall
test('containsForall: ∀a.a → true', () => {
  assert.ok(containsForall(new TForall('a', new TVar('a'))));
});

test('containsForall: Int → false', () => {
  assert.ok(!containsForall(tInt));
});

// Runtime examples
test('applyBoth: identity on [1, "hello"]', () => {
  const id = x => x;
  const [a, b] = applyBoth(id, [42, 'hello']);
  assert.equal(a, 42);
  assert.equal(b, 'hello');
});

test('runST: state token is local', () => {
  const result = runST(s => 42); // s never escapes
  assert.equal(result, 42);
});

test('withFile: handle scope', () => {
  const result = withFile(h => `reading ${h._file}`, 'test.txt');
  assert.equal(result, 'reading test.txt');
});

test('canInfer: rank-1 inferrable', () => {
  const ty = new TForall('a', new TFun(new TVar('a'), new TVar('a')));
  assert.ok(canInfer(ty).inferrable);
});

console.log(`\nHigher-rank types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
