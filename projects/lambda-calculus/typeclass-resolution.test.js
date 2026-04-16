import { strict as assert } from 'assert';
import { TypeClass, Instance, Resolver, createStdLib } from './typeclass-resolution.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('resolve: Eq Int', () => {
  const r = createStdLib();
  assert.ok(r.resolve('Eq', 'Int').ok);
});

test('resolve: Show String', () => {
  const r = createStdLib();
  assert.ok(r.resolve('Show', 'String').ok);
});

test('resolve: missing instance → error', () => {
  const r = createStdLib();
  assert.ok(!r.resolve('Show', 'Float').ok);
});

test('resolve: Eq [Int] with constraint', () => {
  const r = createStdLib();
  assert.ok(r.resolve('Eq', '[Int]').ok);
});

test('resolve: Ord Int checks superclass Eq', () => {
  const r = createStdLib();
  assert.ok(r.resolve('Ord', 'Int').ok);
});

test('resolve: Ord fails without Eq', () => {
  const r = new Resolver();
  r.addClass(new TypeClass('Eq'));
  r.addClass(new TypeClass('Ord', ['Eq']));
  r.addInstance(new Instance('Ord', 'Float', new Map()));
  // No Eq Float → Ord should fail
  assert.ok(!r.resolve('Ord', 'Float').ok);
});

test('dispatch: Show Int show', () => {
  const r = createStdLib();
  const show = r.dispatch('Show', 'Int', 'show');
  assert.equal(show(42), '42');
});

test('dispatch: Eq Int eq', () => {
  const r = createStdLib();
  const eq = r.dispatch('Eq', 'Int', 'eq');
  assert.ok(eq(1, 1));
  assert.ok(!eq(1, 2));
});

test('dispatch: Ord Int compare', () => {
  const r = createStdLib();
  const cmp = r.dispatch('Ord', 'Int', 'compare');
  assert.equal(cmp(1, 2), -1);
  assert.equal(cmp(2, 2), 0);
  assert.equal(cmp(3, 2), 1);
});

test('instancesOf: all Show instances', () => {
  const r = createStdLib();
  const showInstances = r.instancesOf('Show');
  assert.ok(showInstances.length >= 3);
});

test('depth limit: prevents infinite loops', () => {
  const r = new Resolver();
  r.addClass(new TypeClass('A'));
  // Circular: A X requires A X (would loop forever)
  r.addInstance(new Instance('A', 'X', new Map(), [{ className: 'A', typeName: 'X' }]));
  const result = r.resolve('A', 'X');
  assert.ok(!result.ok);
});

console.log(`\nTypeclass resolution tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
