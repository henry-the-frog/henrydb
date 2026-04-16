import { strict as assert } from 'assert';
import { Hole, EApp1, EApp2, EAdd1, EAdd2, plug, decompose, isValue, depth } from './eval-contexts.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('plug: hole', () => assert.equal(plug(new Hole(), 42), 42));
test('plug: EApp1', () => {
  const r = plug(new EApp1(new Hole(), 'arg'), 'fn');
  assert.equal(r.fn, 'fn');
  assert.equal(r.arg, 'arg');
});

test('plug: nested', () => {
  const ctx = new EAdd1(new EAdd1(new Hole(), 3), 4);
  const r = plug(ctx, 2);
  assert.equal(r.tag, 'Add');
  assert.equal(r.left.left, 2);
});

test('decompose: redex at top', () => {
  const expr = { tag: 'Add', left: 2, right: 3 };
  const d = decompose(expr);
  assert.ok(d);
  assert.equal(d.ctx.tag, 'Hole');
});

test('decompose: nested redex', () => {
  const expr = { tag: 'Add', left: { tag: 'Add', left: 1, right: 2 }, right: 3 };
  const d = decompose(expr);
  assert.ok(d);
  assert.equal(d.ctx.tag, 'EAdd1');
});

test('decompose: no redex', () => {
  assert.equal(decompose(42), null);
});

test('isValue: number', () => assert.ok(isValue(42)));
test('isValue: lambda', () => assert.ok(isValue({ tag: 'Lam' })));
test('isValue: app → false', () => assert.ok(!isValue({ tag: 'App' })));

test('depth: hole = 0', () => assert.equal(depth(new Hole()), 0));
test('depth: nested = 2', () => assert.equal(depth(new EApp1(new EApp2('v', new Hole()), 'a')), 2));

console.log(`\nEvaluation contexts tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
