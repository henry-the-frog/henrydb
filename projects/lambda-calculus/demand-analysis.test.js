import { strict as assert } from 'assert';
import { D_ABSENT, D_STRICT, D_HEAD, Var, Num, App, Lam, Case, Let, analyzeDemand, lubDemand, demandString } from './demand-analysis.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('demand: unused = absent', () => assert.equal(analyzeDemand(new Num(42), 'x').tag, 'Absent'));
test('demand: direct use = strict', () => assert.equal(analyzeDemand(new Var('x'), 'x').tag, 'Strict'));
test('demand: in lambda body = absent (not yet evaluated)', () => {
  assert.equal(analyzeDemand(new Lam('y', new Var('x')), 'x').tag, 'Absent');
});

test('demand: case scrutinee = head strict', () => {
  const expr = new Case(new Var('x'), [new Num(1), new Num(2)]);
  assert.equal(analyzeDemand(expr, 'x').tag, 'Head');
});

test('demand: f x = call demand', () => {
  const expr = new App(new Var('f'), new Var('a'));
  const d = analyzeDemand(expr, 'f');
  assert.equal(d.tag, 'Call');
  assert.equal(d.arity, 1);
});

test('demand: f x y → call in fn position', () => {
  const expr = new App(new App(new Var('f'), new Var('a')), new Var('b'));
  const d = analyzeDemand(expr, 'f');
  assert.equal(d.tag, 'Call'); // At least a call demand
});

test('lub: absent ⊔ strict = strict', () => assert.equal(lubDemand(D_ABSENT, D_STRICT).tag, 'Strict'));
test('lub: absent ⊔ absent = absent', () => assert.equal(lubDemand(D_ABSENT, D_ABSENT).tag, 'Absent'));
test('lub: head ⊔ strict = strict', () => assert.equal(lubDemand(D_HEAD, D_STRICT).tag, 'Strict'));

test('demandString: C(2)', () => assert.equal(demandString({ tag: 'Call', arity: 2 }), 'C(2)'));

test('let: demand in init', () => {
  const expr = new Let('y', new Var('x'), new Var('y'));
  assert.equal(analyzeDemand(expr, 'x').tag, 'Strict');
});

console.log(`\nDemand analysis tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
