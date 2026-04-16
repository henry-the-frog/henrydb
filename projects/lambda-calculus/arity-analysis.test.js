import { strict as assert } from 'assert';
import { Var, Num, Lam, App, Let, manifestArity, callArity, minCallArity, etaExpand } from './arity-analysis.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('manifestArity: λx.λy.body = 2', () => {
  assert.equal(manifestArity(new Lam('x', new Lam('y', new Var('x')))), 2);
});
test('manifestArity: non-lambda = 0', () => assert.equal(manifestArity(new Var('x')), 0));
test('manifestArity: single lambda = 1', () => assert.equal(manifestArity(new Lam('x', new Var('x'))), 1));

test('callArity: f x y = [2]', () => {
  const expr = new App(new App(new Var('f'), new Var('x')), new Var('y'));
  assert.deepStrictEqual(callArity(expr, 'f'), [2]);
});

test('callArity: f x = [1]', () => {
  assert.deepStrictEqual(callArity(new App(new Var('f'), new Var('x')), 'f'), [1]);
});

test('callArity: multiple call sites', () => {
  // let _ = f x in f x y
  const expr = new Let('_', new App(new Var('f'), new Var('x')),
    new App(new App(new Var('f'), new Var('x')), new Var('y')));
  const arities = callArity(expr, 'f');
  assert.ok(arities.includes(1));
  assert.ok(arities.includes(2));
});

test('minCallArity: mixed = min', () => {
  const expr = new Let('_', new App(new Var('f'), new Var('x')),
    new App(new App(new Var('f'), new Var('a')), new Var('b')));
  assert.equal(minCallArity(expr, 'f'), 1);
});

test('etaExpand: add 1 lambda', () => {
  const expr = new Var('f');
  const expanded = etaExpand(expr, 1);
  assert.equal(expanded.tag, 'Lam');
  assert.equal(expanded.body.tag, 'App');
});

test('etaExpand: already sufficient → unchanged', () => {
  const expr = new Lam('x', new Var('x'));
  assert.equal(etaExpand(expr, 1).tag, 'Lam');
  assert.equal(manifestArity(etaExpand(expr, 1)), 1);
});

test('etaExpand: add 2 lambdas', () => {
  const expanded = etaExpand(new Var('f'), 2);
  assert.equal(manifestArity(expanded), 2);
});

console.log(`\nArity analysis tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
