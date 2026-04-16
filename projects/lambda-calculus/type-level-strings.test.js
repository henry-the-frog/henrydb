import { strict as assert } from 'assert';
import { TLitStr, TStr, TConcat, TTemplate, tStr, reduce, isSubtype, startsWith, inferTemplate, split, length } from './type-level-strings.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('reduce: concat literals', () => {
  const r = reduce(new TConcat(new TLitStr('hello'), new TLitStr(' world')));
  assert.equal(r.value, 'hello world');
});

test('reduce: template all literal', () => {
  const r = reduce(new TTemplate([new TLitStr('a'), new TLitStr('b'), new TLitStr('c')]));
  assert.equal(r.value, 'abc');
});

test('reduce: template with string → not reduced', () => {
  const r = reduce(new TTemplate([new TLitStr('a'), tStr]));
  assert.equal(r.tag, 'TTemplate');
});

test('subtype: "hello" <: string', () => assert.ok(isSubtype(new TLitStr('hello'), tStr)));
test('subtype: "a" <: "a"', () => assert.ok(isSubtype(new TLitStr('a'), new TLitStr('a'))));
test('subtype: "a" !<: "b"', () => assert.ok(!isSubtype(new TLitStr('a'), new TLitStr('b'))));
test('subtype: string !<: "a"', () => assert.ok(!isSubtype(tStr, new TLitStr('a'))));

test('startsWith: "hello" starts with "hel"', () => assert.ok(startsWith(new TLitStr('hello'), 'hel')));

test('inferTemplate: all literal', () => {
  const r = inferTemplate(['hello ', ' world'], new TLitStr('dear'));
  assert.equal(r.value, 'hello dear world');
});

test('split: "a.b.c" by "."', () => {
  const parts = split(new TLitStr('a.b.c'), '.');
  assert.equal(parts.length, 3);
  assert.equal(parts[0].value, 'a');
});

test('length: "hello" → 5', () => assert.equal(length(new TLitStr('hello')), 5));

console.log(`\nType-level strings tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
