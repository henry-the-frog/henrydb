import { strict as assert } from 'assert';
import { parseFormat, deriveType, typeSignature, printf } from './type-safe-printf.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Format parsing
test('parse: no specifiers', () => {
  const segs = parseFormat('hello world');
  assert.equal(segs.length, 1);
  assert.equal(segs[0].tag, 'Literal');
});

test('parse: single %d', () => {
  const segs = parseFormat('x = %d');
  assert.equal(segs.length, 2);
  assert.equal(segs[1].spec.tag, 'SpecInt');
});

test('parse: multiple specifiers', () => {
  const segs = parseFormat('%s has %d items');
  assert.equal(segs.filter(s => s.tag === 'Spec').length, 2);
});

test('parse: escaped %%', () => {
  const segs = parseFormat('100%%');
  assert.ok(segs.some(s => s.tag === 'Literal' && s.text.includes('%')));
});

// Type derivation
test('deriveType: %s → %d', () => {
  const { params } = deriveType('%s has %d items');
  assert.deepStrictEqual(params, ['String', 'Int']);
});

test('typeSignature: %d + %d = %d', () => {
  assert.equal(typeSignature('%d + %d = %d'), 'Int → Int → Int → String');
});

test('typeSignature: no args', () => {
  assert.equal(typeSignature('hello'), 'String');
});

// Printf execution
test('printf: simple string', () => {
  assert.equal(printf('hello'), 'hello');
});

test('printf: %s substitution', () => {
  assert.equal(printf('%s world', 'hello'), 'hello world');
});

test('printf: %d substitution', () => {
  assert.equal(printf('x = %d', 42), 'x = 42');
});

test('printf: mixed', () => {
  assert.equal(printf('%s has %d items', 'cart', 5), 'cart has 5 items');
});

test('printf: %f float', () => {
  assert.equal(printf('pi = %f', 3.14159), 'pi = 3.14');
});

// Type errors
test('printf: wrong type → error', () => {
  assert.throws(() => printf('%d', 'not a number'), /%d expects integer/);
});

test('printf: too few args → error', () => {
  assert.throws(() => printf('%s %s', 'only one'), /Too few/);
});

test('printf: too many args → error', () => {
  assert.throws(() => printf('%d', 1, 2), /Too many/);
});

console.log(`\nType-safe printf tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
