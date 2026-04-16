import { strict as assert } from 'assert';
import { TypeclassRegistry, createStandardRegistry } from './dict-passing.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Standard instances
// ============================================================

const reg = createStandardRegistry();

test('Show Int: show 42 → "42"', () => {
  assert.equal(reg.dispatch('Show', 'Int', 'show', [42]), '42');
});

test('Show Bool: show true → "True"', () => {
  assert.equal(reg.dispatch('Show', 'Bool', 'show', [true]), 'True');
});

test('Show String: show "hi" → \'"hi"\'', () => {
  assert.equal(reg.dispatch('Show', 'String', 'show', ['hi']), '"hi"');
});

test('Eq Int: eq 5 5 → true', () => {
  assert.equal(reg.dispatch('Eq', 'Int', 'eq', [5, 5]), true);
});

test('Eq Int: eq 5 3 → false', () => {
  assert.equal(reg.dispatch('Eq', 'Int', 'eq', [5, 3]), false);
});

test('Eq Int: neq 5 3 → true', () => {
  assert.equal(reg.dispatch('Eq', 'Int', 'neq', [5, 3]), true);
});

test('Ord Int: lt 3 5 → true', () => {
  assert.equal(reg.dispatch('Ord', 'Int', 'lt', [3, 5]), true);
});

test('Ord Int: compare 5 3 → 1', () => {
  assert.equal(reg.dispatch('Ord', 'Int', 'compare', [5, 3]), 1);
});

test('Num Int: add 3 4 → 7', () => {
  assert.equal(reg.dispatch('Num', 'Int', 'add', [3, 4]), 7);
});

test('Num Int: mul 6 7 → 42', () => {
  assert.equal(reg.dispatch('Num', 'Int', 'mul', [6, 7]), 42);
});

test('Functor Array: fmap (+1) [1,2,3] → [2,3,4]', () => {
  const result = reg.dispatch('Functor', 'Array', 'fmap', [x => x + 1, [1, 2, 3]]);
  assert.deepStrictEqual(result, [2, 3, 4]);
});

test('Functor Maybe: fmap (*2) Just(5) → 10', () => {
  assert.equal(reg.dispatch('Functor', 'Maybe', 'fmap', [x => x * 2, 5]), 10);
});

test('Functor Maybe: fmap (*2) Nothing → null', () => {
  assert.equal(reg.dispatch('Functor', 'Maybe', 'fmap', [x => x * 2, null]), null);
});

// ============================================================
// Dictionary lookup
// ============================================================

test('getDictionary: Show Int exists', () => {
  const dict = reg.getDictionary('Show', 'Int');
  assert.ok(dict);
  assert.equal(dict.className, 'Show');
});

test('missing instance throws', () => {
  assert.throws(() => reg.dispatch('Show', 'Float', 'show', [3.14]), /No instance/);
});

// ============================================================
// Custom class
// ============================================================

test('custom class: Describable', () => {
  const r = new TypeclassRegistry();
  r.defineClass('Describable', [{ name: 'describe', type: 'a -> String' }]);
  r.addInstance('Describable', 'Point', { describe: p => `(${p.x}, ${p.y})` });
  assert.equal(r.dispatch('Describable', 'Point', 'describe', [{ x: 1, y: 2 }]), '(1, 2)');
});

// ============================================================
// Report
// ============================================================

console.log(`\nTypeclass dictionary passing tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
