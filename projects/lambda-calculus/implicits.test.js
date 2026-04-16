import { strict as assert } from 'assert';
import { ImplicitScope, show, eq, standardScope } from './implicits.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('provide and resolve', () => {
  const s = new ImplicitScope();
  s.provide('Foo', 42);
  assert.equal(s.resolve('Foo'), 42);
});

test('resolve: not found → error', () => {
  assert.throws(() => new ImplicitScope().resolve('Missing'), /No implicit/);
});

test('child scope: inherits', () => {
  const parent = new ImplicitScope();
  parent.provide('X', 1);
  assert.equal(parent.child().resolve('X'), 1);
});

test('child scope: shadows', () => {
  const parent = new ImplicitScope();
  parent.provide('X', 1);
  const child = parent.child();
  child.provide('X', 2);
  assert.equal(child.resolve('X'), 2);
});

test('incoherence: duplicate → error', () => {
  const s = new ImplicitScope();
  s.provide('X', 1);
  assert.throws(() => s.provide('X', 2), /Incoherent/);
});

test('has: true', () => {
  const s = new ImplicitScope();
  s.provide('A', 1);
  assert.ok(s.has('A'));
});

test('has: false', () => assert.ok(!new ImplicitScope().has('Z')));

test('show: number', () => assert.equal(show(standardScope(), 42), '42'));
test('show: string', () => assert.equal(show(standardScope(), 'hello'), '"hello"'));
test('eq: numbers', () => assert.ok(eq(standardScope(), 5, 5)));
test('eq: strings', () => assert.ok(!eq(standardScope(), 'a', 'b')));

console.log(`\nImplicits tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
