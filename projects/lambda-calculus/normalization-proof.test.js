import { strict as assert } from 'assert';
import { TBase, TFun, Var, Lam, App, normalize, typecheck, verifyStrongNormalization, isStronglyNormalizing } from './normalization-proof.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const tInt = new TBase('Int');
const I = new Lam('x', tInt, new Var('x'));
const K = new Lam('x', tInt, new Lam('y', tInt, new Var('x')));

test('typecheck: identity', () => assert.ok(typecheck(I).ok));
test('typecheck: K combinator', () => assert.ok(typecheck(K).ok));
test('typecheck: unbound → error', () => assert.ok(!typecheck(new Var('z')).ok));

test('normalize: identity applied', () => {
  const r = normalize(new App(I, new Var('a')));
  assert.ok(r.normal);
  assert.equal(r.term.name, 'a');
});

test('normalize: K a b → a', () => {
  const r = normalize(new App(new App(K, new Var('a')), new Var('b')));
  assert.ok(r.normal);
  assert.equal(r.term.name, 'a');
});

test('normalize: already normal', () => {
  assert.ok(normalize(new Var('x')).normal);
});

test('SN theorem: identity', () => {
  const r = verifyStrongNormalization(I);
  assert.ok(r.theorem);
});

test('SN theorem: K', () => {
  const r = verifyStrongNormalization(K);
  assert.ok(r.theorem);
});

test('SN theorem: (K I) I', () => {
  const r = verifyStrongNormalization(new App(new App(K, I), I));
  assert.ok(r.theorem);
});

test('isStronglyNormalizing: well-typed → true', () => {
  assert.ok(isStronglyNormalizing(new App(I, new Var('x'))));
});

console.log(`\n🎉🎉🎉 MODULE #130! Normalization proof tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
