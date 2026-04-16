import { strict as assert } from 'assert';
import { NameSupply, avoid, alpha } from './name-supply.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('fresh: sequential names', () => {
  const s = new NameSupply();
  assert.equal(s.fresh(), 'x0');
  assert.equal(s.fresh(), 'x1');
  assert.equal(s.fresh(), 'x2');
});

test('fresh: custom prefix', () => {
  const s = new NameSupply('t');
  assert.equal(s.fresh(), 't0');
});

test('fresh: hint', () => {
  const s = new NameSupply();
  assert.ok(s.fresh('alpha').startsWith('alpha'));
});

test('freshN: multiple at once', () => {
  const s = new NameSupply();
  const names = s.freshN(3);
  assert.equal(names.length, 3);
  assert.notEqual(names[0], names[1]);
});

test('reserve: avoids reserved names', () => {
  const s = new NameSupply();
  s.reserve('x0');
  assert.notEqual(s.fresh(), 'x0');
});

test('child: independent counter', () => {
  const parent = new NameSupply('p');
  const child = parent.child('c');
  assert.equal(child.fresh(), 'c0');
  assert.equal(parent.fresh(), 'p0');
});

test('snapshot/restore: backtrack', () => {
  const s = new NameSupply();
  s.fresh(); s.fresh();
  const snap = s.snapshot();
  s.fresh(); s.fresh();
  s.restore(snap);
  assert.equal(s.fresh(), 'x2');
});

test('avoid: no conflict → same name', () => {
  assert.equal(avoid('x', new Set(['y', 'z'])), 'x');
});

test('avoid: conflict → rename', () => {
  assert.notEqual(avoid('x', new Set(['x'])), 'x');
});

test('alpha: rename to avoid conflict', () => {
  const expr = { tag: 'Lam', var: 'x', body: { tag: 'Var', name: 'x' } };
  const result = alpha(expr, new Set(['x']), new NameSupply());
  assert.notEqual(result.var, 'x');
  assert.equal(result.body.name, result.var); // Body updated too
});

console.log(`\nName supply tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
