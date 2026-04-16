import { strict as assert } from 'assert';
import { inferType, fromJsonSchema, toDeclaration } from './type-providers.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('infer: string', () => assert.equal(inferType('hello').tag, 'TStr'));
test('infer: number', () => assert.equal(inferType(42).tag, 'TNum'));
test('infer: boolean', () => assert.equal(inferType(true).tag, 'TBool'));
test('infer: null', () => assert.equal(inferType(null).tag, 'TNull'));
test('infer: array', () => assert.equal(inferType([1, 2, 3]).tag, 'TArr'));
test('infer: object', () => {
  const t = inferType({ name: 'Alice', age: 30 });
  assert.equal(t.tag, 'TObj');
  assert.equal(t.fields.name.tag, 'TStr');
  assert.equal(t.fields.age.tag, 'TNum');
});
test('infer: mixed array → union', () => {
  const t = inferType([1, 'hello', true]);
  assert.equal(t.elem.tag, 'TUnion');
});

test('fromJsonSchema: object', () => {
  const t = fromJsonSchema({ type: 'object', properties: { id: { type: 'integer' }, name: { type: 'string' } } });
  assert.equal(t.tag, 'TObj');
  assert.equal(t.fields.id.tag, 'TNum');
});

test('toDeclaration: interface', () => {
  const t = inferType({ name: 'Alice', age: 30 });
  const decl = toDeclaration('User', t);
  assert.ok(decl.includes('interface User'));
  assert.ok(decl.includes('name: string'));
});

test('infer: nested object', () => {
  const t = inferType({ user: { name: 'Alice', scores: [1, 2, 3] } });
  assert.equal(t.fields.user.fields.scores.tag, 'TArr');
});

console.log(`\nType providers tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
