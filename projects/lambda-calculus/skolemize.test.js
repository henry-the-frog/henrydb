import { strict as assert } from 'assert';
import {
  TVar, TSkolem, TForall, TFun, TBase, tInt, tBool,
  skolemize, substitute, subsumes, freeVars, resetCounters
} from './skolemize.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { resetCounters(); fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Skolemization
test('skolemize: ∀a.a→a → sk_a0→sk_a0', () => {
  const ty = new TForall('a', new TFun(new TVar('a'), new TVar('a')));
  const { type, skolems } = skolemize(ty);
  assert.equal(type.tag, 'TFun');
  assert.equal(type.param.tag, 'TSkolem');
  assert.equal(type.ret.tag, 'TSkolem');
  assert.equal(skolems.length, 1);
});

test('skolemize: nested ∀a.∀b.a→b', () => {
  const ty = new TForall('a', new TForall('b', new TFun(new TVar('a'), new TVar('b'))));
  const { type, skolems } = skolemize(ty);
  assert.equal(skolems.length, 2);
  assert.notEqual(type.param.id, type.ret.id); // Different skolems
});

test('skolemize: non-forall unchanged', () => {
  const { type, skolems } = skolemize(tInt);
  assert.equal(type.tag, 'TBase');
  assert.equal(skolems.length, 0);
});

// Substitution
test('substitute: a[a:=Int] → Int', () => {
  const result = substitute(new TVar('a'), 'a', tInt);
  assert.equal(result.tag, 'TBase');
  assert.equal(result.name, 'Int');
});

test('substitute: (a→b)[a:=Int] → Int→b', () => {
  const result = substitute(new TFun(new TVar('a'), new TVar('b')), 'a', tInt);
  assert.equal(result.param.name, 'Int');
  assert.equal(result.ret.name, 'b');
});

// Subsumption
test('subsumes: ∀a.a→a subsumes Int→Int', () => {
  const poly = new TForall('a', new TFun(new TVar('a'), new TVar('a')));
  assert.ok(subsumes(poly, new TFun(tInt, tInt)));
});

test('subsumes: Int→Int does NOT subsume ∀a.a→a', () => {
  const poly = new TForall('a', new TFun(new TVar('a'), new TVar('a')));
  assert.ok(!subsumes(new TFun(tInt, tInt), poly));
});

test('subsumes: same monotype', () => {
  assert.ok(subsumes(new TFun(tInt, tBool), new TFun(tInt, tBool)));
});

test('subsumes: ∀a.a→a subsumes ∀b.b→b', () => {
  const poly1 = new TForall('a', new TFun(new TVar('a'), new TVar('a')));
  const poly2 = new TForall('b', new TFun(new TVar('b'), new TVar('b')));
  assert.ok(subsumes(poly1, poly2));
});

// Free variables
test('freeVars: a→b has {a,b}', () => {
  const fv = freeVars(new TFun(new TVar('a'), new TVar('b')));
  assert.ok(fv.has('a'));
  assert.ok(fv.has('b'));
});

test('freeVars: ∀a.a→b has {b}', () => {
  const fv = freeVars(new TForall('a', new TFun(new TVar('a'), new TVar('b'))));
  assert.ok(!fv.has('a'));
  assert.ok(fv.has('b'));
});

console.log(`\nSkolemization tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
