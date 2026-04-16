import { strict as assert } from 'assert';
import { PVar, PLam, PApp, PPair, PFst, PSnd, PInl, PInr, checkProof, evalProof } from './proof-terms.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// A → A: identity
test('proof: A → A (identity)', () => {
  assert.ok(checkProof(new PLam('x', new PVar('x')), 'A → A'));
});

// A → B → A: const
test('proof: A → B → A (const)', () => {
  assert.ok(checkProof(new PLam('a', new PLam('b', new PVar('a'))), 'A → B → A'));
});

// A ∧ B from A and B
test('proof: A → B → A ∧ B (pair intro)', () => {
  const term = new PLam('a', new PLam('b', new PPair(new PVar('a'), new PVar('b'))));
  assert.ok(checkProof(term, 'A → B → A ∧ B'));
});

// A ∨ B from A (left)
test('proof: A → A ∨ B (inl)', () => {
  assert.ok(checkProof(new PLam('a', new PInl(new PVar('a'))), 'A → A ∨ B'));
});

// A ∨ B from B (right)
test('proof: B → A ∨ B (inr)', () => {
  assert.ok(checkProof(new PLam('b', new PInr(new PVar('b'))), 'B → A ∨ B'));
});

// Invalid proof
test('invalid: A → B (no proof without B)', () => {
  assert.ok(!checkProof(new PLam('a', new PVar('b')), 'A → B'));
});

// Eval: identity
test('eval: identity applied', () => {
  const id = evalProof(new PLam('x', new PVar('x')));
  assert.equal(id(42), 42);
});

// Eval: const
test('eval: const', () => {
  const k = evalProof(new PLam('a', new PLam('b', new PVar('a'))));
  assert.equal(k(1)(2), 1);
});

// Eval: pair
test('eval: pair', () => {
  const pair = evalProof(new PPair(new PVar('a'), new PVar('b')), new Map([['a', 1], ['b', 2]]));
  assert.deepStrictEqual(pair, [1, 2]);
});

// Eval: fst/snd
test('eval: fst of pair', () => {
  const r = evalProof(new PFst(new PPair(new PVar('a'), new PVar('b'))), new Map([['a', 10], ['b', 20]]));
  assert.equal(r, 10);
});

test('eval: inl', () => {
  const r = evalProof(new PInl(new PVar('x')), new Map([['x', 42]]));
  assert.deepStrictEqual(r, { tag: 'Left', value: 42 });
});

test('eval: inr', () => {
  const r = evalProof(new PInr(new PVar('x')), new Map([['x', 99]]));
  assert.deepStrictEqual(r, { tag: 'Right', value: 99 });
});

// 🎉🎉🎉 THIS IS TEST #2000!!!
test('🎉 2000th TEST: A → B → B (flip-const)', () => {
  assert.ok(checkProof(new PLam('a', new PLam('b', new PVar('b'))), 'A → B → B'));
});

console.log(`\n🎉🎉🎉 Proof terms tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
