import { strict as assert } from 'assert';
import { Var, Lam, App, zipper, leftmost, rightmost } from './zipper.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const term = new App(new Lam('x', new Var('x')), new Var('y'));

test('zipper: focus on root', () => {
  const z = zipper(term);
  assert.equal(z.focus.tag, 'App');
  assert.ok(z.isTop());
});

test('down: into fn', () => {
  const z = zipper(term).down();
  assert.equal(z.focus.tag, 'Lam');
});

test('downRight: into arg', () => {
  const z = zipper(term).downRight();
  assert.equal(z.focus.name, 'y');
});

test('up: back to root', () => {
  const z = zipper(term).down().up();
  assert.equal(z.focus.tag, 'App');
  assert.ok(z.isTop());
});

test('replace: change focused node', () => {
  const z = zipper(term).downRight().replace(new Var('z'));
  assert.equal(z.toTerm().toString(), '((λx.x) z)');
});

test('modify: transform focused node', () => {
  const z = zipper(term).down().down(); // Into lambda body
  const modified = z.modify(n => new App(n, new Var('w')));
  assert.equal(modified.focus.tag, 'App');
});

test('toTerm: reconstruct', () => {
  const z = zipper(term).down().down(); // Deep
  assert.equal(z.toTerm().toString(), term.toString());
});

test('depth: tracks position', () => {
  assert.equal(zipper(term).depth(), 0);
  assert.equal(zipper(term).down().depth(), 1);
  assert.equal(zipper(term).down().down().depth(), 2);
});

test('leftmost: finds deepest left', () => {
  const z = leftmost(zipper(term));
  assert.equal(z.focus.tag, 'Var');
});

test('rightmost: finds deepest right', () => {
  const z = rightmost(zipper(term));
  assert.equal(z.focus.name, 'y');
});

test('down from var: returns null', () => {
  assert.equal(zipper(new Var('x')).down(), null);
});

console.log(`\nZipper tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
