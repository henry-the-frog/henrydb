import { strict as assert } from 'assert';
import { Var, Num, Lam, App, Let, BinOp, exprSize, usageCount, shouldInline, applyInlining, INLINE_ALWAYS, INLINE_NEVER } from './inlining.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('exprSize: var = 1', () => assert.equal(exprSize(new Var('x')), 1));
test('exprSize: app = 3', () => assert.equal(exprSize(new App(new Var('f'), new Var('x'))), 3));

test('usageCount: x in x+y = 1', () => {
  assert.equal(usageCount(new BinOp('+', new Var('x'), new Var('y')), 'x'), 1);
});

test('usageCount: x in x+x = 2', () => {
  assert.equal(usageCount(new BinOp('+', new Var('x'), new Var('x')), 'x'), 2);
});

test('shouldInline: dead → always', () => {
  assert.equal(shouldInline('x', new Num(5), new Num(42)).decision, INLINE_ALWAYS);
});

test('shouldInline: single use → always', () => {
  assert.equal(shouldInline('x', new Num(5), new Var('x')).decision, INLINE_ALWAYS);
});

test('shouldInline: trivial → always', () => {
  assert.equal(shouldInline('x', new Var('y'), new BinOp('+', new Var('x'), new Var('x'))).decision, INLINE_ALWAYS);
});

test('shouldInline: large+many → never', () => {
  const big = new BinOp('+', new BinOp('*', new Var('a'), new Var('b')), new BinOp('-', new Var('c'), new Var('d')));
  const body = new BinOp('+', new BinOp('+', new Var('x'), new Var('x')), new BinOp('+', new Var('x'), new Var('x')));
  assert.equal(shouldInline('x', big, body).decision, INLINE_NEVER);
});

test('applyInlining: dead let removed', () => {
  const r = applyInlining(new Let('x', new Num(5), new Num(42)));
  assert.equal(r.n, 42);
});

test('applyInlining: single use inlined', () => {
  const r = applyInlining(new Let('x', new Num(5), new BinOp('+', new Var('x'), new Num(1))));
  // x should be replaced with 5
  assert.equal(r.left.n, 5);
});

console.log(`\nInlining heuristics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
