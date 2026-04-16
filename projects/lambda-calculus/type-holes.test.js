import { strict as assert } from 'assert';
import { THole, TCon, HoleContext, inferWithHoles, ENum, EBool, EVar, ELam, EHole } from './type-holes.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('createHole: returns THole', () => {
  const ctx = new HoleContext();
  const h = ctx.createHole();
  assert.equal(h.tag, 'THole');
});

test('solve: fills hole', () => {
  const ctx = new HoleContext();
  const h = ctx.createHole();
  ctx.solve(h.id, new TCon('Int'));
  assert.ok(ctx.isSolved(h.id));
  assert.equal(h.toString(), 'Int');
});

test('unsolved: lists unresolved', () => {
  const ctx = new HoleContext();
  ctx.createHole('test');
  assert.equal(ctx.unsolved().length, 1);
});

test('allSolved: false with holes', () => {
  const ctx = new HoleContext();
  ctx.createHole();
  assert.ok(!ctx.allSolved());
});

test('allSolved: true when all filled', () => {
  const ctx = new HoleContext();
  const h = ctx.createHole();
  ctx.solve(h.id, new TCon('Bool'));
  assert.ok(ctx.allSolved());
});

test('fillAll: batch solve', () => {
  const ctx = new HoleContext();
  ctx.createHole(); ctx.createHole();
  ctx.fillAll({ 0: new TCon('Int'), 1: new TCon('Bool') });
  assert.ok(ctx.allSolved());
});

test('infer: num → Int', () => {
  const ctx = new HoleContext();
  const t = inferWithHoles(new ENum(42), new Map(), ctx);
  assert.equal(t.name, 'Int');
});

test('infer: hole → THole', () => {
  const ctx = new HoleContext();
  const t = inferWithHoles(new EHole('expected Int'), new Map(), ctx);
  assert.equal(t.tag, 'THole');
});

test('infer: lambda creates param hole', () => {
  const ctx = new HoleContext();
  const t = inferWithHoles(new ELam('x', new EVar('x')), new Map(), ctx);
  assert.equal(t.tag, 'TFun');
});

test('unsolved: shows context hints', () => {
  const ctx = new HoleContext();
  ctx.createHole('my hint');
  assert.ok(ctx.unsolved()[0].suggestion.includes('my hint'));
});

console.log(`\nType holes tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
