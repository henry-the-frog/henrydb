import { strict as assert } from 'assert';
import { Var, Num, App, BinOp, exprKey, findCSE, eliminateCSE, resetCSE } from './cse.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { resetCSE(); fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('exprKey: unique for different exprs', () => {
  assert.notEqual(exprKey(new Var('x')), exprKey(new Var('y')));
});

test('exprKey: same for identical exprs', () => {
  assert.equal(exprKey(new App(new Var('f'), new Var('x'))), exprKey(new App(new Var('f'), new Var('x'))));
});

test('findCSE: detect duplicate f(x)', () => {
  const fx = new App(new Var('f'), new Var('x'));
  const expr = new BinOp('+', fx, new App(new Var('f'), new Var('x')));
  assert.ok(findCSE(expr).length > 0);
});

test('findCSE: no duplicates', () => {
  const expr = new BinOp('+', new Var('x'), new Var('y'));
  assert.equal(findCSE(expr).length, 0);
});

test('eliminateCSE: f(x) + f(x) → let t = f(x) in t + t', () => {
  const expr = new BinOp('+', new App(new Var('f'), new Var('x')), new App(new Var('f'), new Var('x')));
  const r = eliminateCSE(expr);
  assert.equal(r.tag, 'Let');
  assert.equal(r.body.left.tag, 'Var');
  assert.equal(r.body.right.tag, 'Var');
  assert.equal(r.body.left.name, r.body.right.name); // Same var!
});

test('eliminateCSE: no duplicates → unchanged', () => {
  const expr = new BinOp('+', new Var('x'), new Var('y'));
  const r = eliminateCSE(expr);
  assert.equal(r.tag, 'BinOp');
});

test('eliminateCSE: vars/nums not shared', () => {
  const expr = new BinOp('+', new Var('x'), new Var('x'));
  const r = eliminateCSE(expr);
  assert.equal(r.tag, 'BinOp'); // Variables are trivial, no CSE
});

test('eliminateCSE: nested common subexpr', () => {
  const sub = new BinOp('+', new Var('a'), new Var('b'));
  const expr = new BinOp('*', sub, new BinOp('+', new Var('a'), new Var('b')));
  const r = eliminateCSE(expr);
  assert.equal(r.tag, 'Let');
});

test('exprKey: BinOp includes operator', () => {
  assert.notEqual(exprKey(new BinOp('+', new Num(1), new Num(2))), exprKey(new BinOp('*', new Num(1), new Num(2))));
});

test('eliminateCSE: deeply nested', () => {
  const sub = new App(new Var('g'), new Num(1));
  const expr = new BinOp('+', new App(new Var('f'), sub), new App(new Var('f'), new App(new Var('g'), new Num(1))));
  const r = eliminateCSE(expr);
  assert.equal(r.tag, 'Let');
});

console.log(`\nCSE tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
