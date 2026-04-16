import { strict as assert } from 'assert';
import { Var, Num, Lam, App, Add, Let, CostEval, estimateCost } from './cost-semantics.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('eval: number', () => assert.equal(new CostEval().run(new Num(42)).result, 42));
test('eval: add', () => assert.equal(new CostEval().run(new Add(new Num(2), new Num(3))).result, 5));
test('eval: identity', () => assert.equal(new CostEval().run(new App(new Lam('x', new Var('x')), new Num(42))).result, 42));

test('cost: number = 0 reductions', () => {
  const r = new CostEval().run(new Num(42));
  assert.equal(r.costs.beta, 0);
});

test('cost: app = 1 beta reduction', () => {
  const r = new CostEval().run(new App(new Lam('x', new Var('x')), new Num(42)));
  assert.equal(r.costs.beta, 1);
});

test('cost: 2 apps = 2 betas', () => {
  const K = new Lam('x', new Lam('y', new Var('x')));
  const r = new CostEval().run(new App(new App(K, new Num(1)), new Num(2)));
  assert.equal(r.costs.beta, 2);
});

test('cost: add counted', () => {
  assert.equal(new CostEval().run(new Add(new Num(1), new Num(2))).costs.add, 1);
});

test('cost: let counted as alloc', () => {
  const r = new CostEval().run(new Let('x', new Num(5), new Var('x')));
  assert.ok(r.costs.alloc > 0);
});

test('cost: total is sum', () => {
  const r = new CostEval().run(new App(new Lam('x', new Add(new Var('x'), new Num(1))), new Num(41)));
  assert.equal(r.costs.total, r.costs.beta + r.costs.alloc + r.costs.lookup + r.costs.add);
});

test('estimateCost: larger for complex', () => {
  assert.ok(estimateCost(new App(new Lam('x', new Var('x')), new Num(42))) > estimateCost(new Num(42)));
});

console.log(`\nCost semantics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
