import { strict as assert } from 'assert';
import { Selective, select, ifS, whenS, branch, isStaticRight, isStaticLeft } from './selective.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('pure: wraps in Right', () => assert.equal(Selective.pure(42).value.tag, 'Right'));
test('left: wraps in Left', () => assert.equal(Selective.left(42).value.tag, 'Left'));

test('select: Right → skip handler', () => {
  const r = select(Selective.right(42), Selective.pure(x => x * 2));
  assert.equal(r.value.value, 42); // Handler not applied
});

test('select: Left → apply handler', () => {
  const r = select(Selective.left(21), Selective.pure(x => x * 2));
  assert.equal(r.value.value, 42);
});

test('map: Right', () => {
  const r = Selective.right(21).map(x => x * 2);
  assert.equal(r.value.value, 42);
});

test('map: Left → unchanged', () => {
  const r = Selective.left(21).map(x => x * 2);
  assert.equal(r.value.value, 21); // Not mapped
});

test('ifS: true → then', () => {
  const r = ifS(Selective.right(true), Selective.pure(1), Selective.pure(2));
  assert.equal(r.value.value, 1);
});

test('ifS: false → else', () => {
  const r = ifS(Selective.right(false), Selective.pure(1), Selective.pure(2));
  assert.equal(r.value.value, 2);
});

test('whenS: true → run action', () => {
  const r = whenS(Selective.right(true), Selective.pure('done'));
  assert.equal(r.value.value, 'done');
});

test('whenS: false → skip', () => {
  const r = whenS(Selective.right(false), Selective.pure('done'));
  assert.equal(r.value.value, null);
});

test('branch: Left → fLeft', () => {
  const r = branch(Selective.left(5), Selective.pure(x => x * 2), Selective.pure(x => x + 1));
  assert.equal(r.value.value, 10);
});

test('isStaticRight/Left', () => {
  assert.ok(isStaticRight(Selective.right(1)));
  assert.ok(isStaticLeft(Selective.left(1)));
  assert.ok(!isStaticRight(Selective.left(1)));
});

console.log(`\nSelective functors tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
