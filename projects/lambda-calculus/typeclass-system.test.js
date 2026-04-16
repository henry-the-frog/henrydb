import { strict as assert } from 'assert';
import { TypeClass, Instance, ClassEnv } from './typeclass-system.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const env = new ClassEnv();

test('addClass: Eq', () => {
  env.addClass(new TypeClass('Eq', ['a'], [], ['eq', 'neq']));
  assert.ok(env.classes.has('Eq'));
});

test('addClass: Ord with superclass Eq', () => {
  env.addClass(new TypeClass('Ord', ['a'], ['Eq'], ['compare']));
  assert.ok(env.classes.has('Ord'));
});

test('addInstance: Eq Int', () => {
  env.addInstance(new Instance('Eq', ['Int'], [], { eq: (a, b) => a === b }));
  assert.equal(env.instances.length, 1);
});

test('addInstance: Eq String', () => {
  env.addInstance(new Instance('Eq', ['String'], [], { eq: (a, b) => a === b }));
  assert.equal(env.instances.length, 2);
});

test('resolve: Eq Int → found', () => {
  const r = env.resolve('Eq', 'Int');
  assert.ok(r);
  assert.equal(r.className, 'Eq');
});

test('resolve: Eq Bool → null (no instance)', () => {
  assert.equal(env.resolve('Eq', 'Bool'), null);
});

test('hasSuperclass: Ord has Eq', () => {
  assert.ok(env.hasSuperclass('Ord', 'Eq'));
});

test('hasSuperclass: Eq does not have Ord', () => {
  assert.ok(!env.hasSuperclass('Eq', 'Ord'));
});

test('overlapping instances → error', () => {
  const env2 = new ClassEnv();
  env2.addInstance(new Instance('Show', ['Int']));
  assert.throws(() => env2.addInstance(new Instance('Show', ['Int'])), /Overlapping/);
});

test('instance with constraints', () => {
  const env2 = new ClassEnv();
  env2.addInstance(new Instance('Eq', ['Int']));
  env2.addInstance(new Instance('Ord', ['Int'], [{ class: 'Eq' }]));
  const r = env2.resolve('Ord', 'Int');
  assert.ok(r);
});

console.log(`\nTypeclass system tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
