import { strict as assert } from 'assert';
import { Rule, RuleEngine, Var, Num, App, Lam, BinOp, doubleNeg, addZero, mulOne, constFold } from './rewrite-rules.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const engine = new RuleEngine();
engine.addRule(doubleNeg).addRule(addZero).addRule(mulOne).addRule(constFold);

test('constFold: 2 + 3 → 5', () => {
  const r = engine.rewrite(new BinOp('+', new Num(2), new Num(3)));
  assert.equal(r.result.n, 5);
});

test('addZero: x + 0 → x', () => {
  const r = engine.rewrite(new BinOp('+', new Var('x'), new Num(0)));
  assert.equal(r.result.name, 'x');
});

test('mulOne: x * 1 → x', () => {
  const r = engine.rewrite(new BinOp('*', new Var('x'), new Num(1)));
  assert.equal(r.result.name, 'x');
});

test('doubleNeg: neg(neg(x)) → x', () => {
  const expr = new App(new Var('neg'), new App(new Var('neg'), new Var('x')));
  const r = engine.rewrite(expr);
  assert.equal(r.result.name, 'x');
});

test('deep rewrite: (2 + 3) * 1 → 5', () => {
  const expr = new BinOp('*', new BinOp('+', new Num(2), new Num(3)), new Num(1));
  const r = engine.rewrite(expr);
  assert.equal(r.result.n, 5);
});

test('no rule matches → unchanged', () => {
  const expr = new Var('x');
  const r = engine.rewrite(expr);
  assert.equal(r.result.name, 'x');
  assert.equal(r.steps, 0);
});

test('fired rules tracked', () => {
  const myFold = new Rule('const-fold2',
    e => { if (e.tag !== 'BinOp' || e.left.tag !== 'Num' || e.right.tag !== 'Num') return null; return { op: e.op, a: e.left.n, b: e.right.n }; },
    b => new Num(b.op === '+' ? b.a + b.b : b.a * b.b));
  const e = new RuleEngine();
  e.addRule(myFold);
  e.rewrite(new BinOp('+', new Num(1), new Num(2)));
  assert.equal(e.stats()[0].fires, 1);
});

test('multiple fires', () => {
  const e = new RuleEngine().addRule(constFold);
  e.rewrite(new BinOp('+', new BinOp('+', new Num(1), new Num(2)), new Num(3)));
  assert.ok(e.stats()[0].fires >= 2);
});

test('custom rule', () => {
  const e = new RuleEngine();
  e.addRule(new Rule('id-elim', expr => expr.tag === 'App' && expr.fn.tag === 'Var' && expr.fn.name === 'id' ? { x: expr.arg } : null, b => b.x));
  const r = e.rewrite(new App(new Var('id'), new Num(42)));
  assert.equal(r.result.n, 42);
});

test('inside lambda', () => {
  const e = new RuleEngine().addRule(constFold);
  const r = e.rewrite(new Lam('x', new BinOp('+', new Num(1), new Num(2))));
  assert.equal(r.result.body.n, 3);
});

console.log(`\nRewrite rules tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
