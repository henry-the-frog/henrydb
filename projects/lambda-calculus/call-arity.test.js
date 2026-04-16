import { strict as assert } from 'assert';
import { Var, Num, Lam, App, Let, If, arity, appArity, analyzeCallArity, suggestArity } from './call-arity.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('arity: λx.λy.x = 2', () => assert.equal(arity(new Lam('x', new Lam('y', new Var('x')))), 2));
test('arity: var = 0', () => assert.equal(arity(new Var('x')), 0));

test('appArity: f x y = {args:2, head:f}', () => {
  const { args, head } = appArity(new App(new App(new Var('f'), new Var('x')), new Var('y')));
  assert.equal(args, 2);
  assert.equal(head.name, 'f');
});

test('analyzeCallArity: consistent calls', () => {
  // f = λx.λy. if ... then f a b else ...
  const body = new Lam('x', new Lam('y', new If(new Var('c'), new App(new App(new Var('f'), new Var('a')), new Var('b')), new Num(0))));
  const r = analyzeCallArity('f', body);
  assert.equal(r.callSites, 1);
  assert.equal(r.minimum, 2);
  assert.ok(r.consistent);
});

test('analyzeCallArity: no recursive calls', () => {
  const body = new Lam('x', new Var('x'));
  const r = analyzeCallArity('f', body);
  assert.equal(r.callSites, 0);
});

test('analyzeCallArity: mixed arities', () => {
  const body = new Let('_', new App(new Var('f'), new Var('x')),
    new App(new App(new Var('f'), new Var('a')), new Var('b')));
  const r = analyzeCallArity('f', body);
  assert.ok(!r.consistent);
  assert.equal(r.minimum, 1);
  assert.equal(r.maximum, 2);
});

test('suggestArity: consistent', () => {
  const def = new Lam('x', new Lam('y', new App(new App(new Var('f'), new Var('a')), new Var('b'))));
  const r = suggestArity('f', def);
  assert.equal(r.optimal, 2);
});

test('suggestArity: no recursion', () => {
  const def = new Lam('x', new Var('x'));
  const r = suggestArity('f', def);
  assert.equal(r.optimal, 1);
});

test('appArity: single app = 1', () => {
  assert.equal(appArity(new App(new Var('f'), new Var('x'))).args, 1);
});

test('appArity: non-app = 0', () => {
  assert.equal(appArity(new Var('x')).args, 0);
});

console.log(`\nCall arity tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
