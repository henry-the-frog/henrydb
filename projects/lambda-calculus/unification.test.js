import { strict as assert } from 'assert';
import { TVar, TFun, TCon, applySubst, occursIn, unify, unifyAll, freeVars } from './unification.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const a = new TVar('a'), b = new TVar('b'), c = new TVar('c');
const tInt = new TCon('Int'), tBool = new TCon('Bool');

test('unify: same var', () => {
  const r = unify(a, a);
  assert.ok(r.ok);
  assert.equal(r.subst.size, 0);
});

test('unify: var with concrete', () => {
  const r = unify(a, tInt);
  assert.ok(r.ok);
  assert.equal(r.subst.get('a').name, 'Int');
});

test('unify: fun types', () => {
  const r = unify(new TFun(a, b), new TFun(tInt, tBool));
  assert.ok(r.ok);
  assert.equal(applySubst(r.subst, a).name, 'Int');
  assert.equal(applySubst(r.subst, b).name, 'Bool');
});

test('unify: nested functions', () => {
  const r = unify(new TFun(a, new TFun(b, c)), new TFun(tInt, new TFun(tBool, tInt)));
  assert.ok(r.ok);
});

test('unify: occurs check fails', () => {
  const r = unify(a, new TFun(a, tInt));
  assert.ok(!r.ok);
  assert.ok(r.error.includes('Occurs'));
});

test('unify: different constructors fail', () => {
  const r = unify(tInt, tBool);
  assert.ok(!r.ok);
});

test('unify: parameterized types', () => {
  const listA = new TCon('List', [a]);
  const listInt = new TCon('List', [tInt]);
  const r = unify(listA, listInt);
  assert.ok(r.ok);
  assert.equal(applySubst(r.subst, a).name, 'Int');
});

test('unify: arity mismatch', () => {
  const r = unify(new TCon('Pair', [a, b]), new TCon('Pair', [tInt]));
  assert.ok(!r.ok);
});

test('occursIn: positive', () => assert.ok(occursIn('a', new TFun(a, tInt))));
test('occursIn: negative', () => assert.ok(!occursIn('a', new TFun(b, tInt))));

test('unifyAll: multiple constraints', () => {
  const r = unifyAll([
    [a, tInt],
    [b, new TFun(a, tBool)],
  ]);
  assert.ok(r.ok);
  const resolvedB = applySubst(r.subst, b);
  assert.equal(resolvedB.tag, 'TFun');
});

test('freeVars: a → b has {a, b}', () => {
  const fv = freeVars(new TFun(a, b));
  assert.ok(fv.has('a'));
  assert.ok(fv.has('b'));
});

console.log(`\nUnification tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
