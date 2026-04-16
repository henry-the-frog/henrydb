import { strict as assert } from 'assert';
import { TRow, rowExtend, rowRemove, rowSelect, rowRename, rowMerge, rowDiff, rowIntersect } from './row-ops.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const r = new TRow({ x: 'Int', y: 'Bool', z: 'String' });

test('extend: add field', () => assert.ok(rowExtend(new TRow({}), 'a', 'Int').fields.has('a')));
test('remove: delete field', () => assert.ok(!rowRemove(r, 'x').fields.has('x')));
test('remove: missing → error', () => assert.throws(() => rowRemove(r, 'w'), /not in row/));
test('select: pick fields', () => {
  const s = rowSelect(r, ['x', 'z']);
  assert.equal(s.fields.size, 2);
  assert.ok(s.fields.has('x'));
});
test('rename: x → a', () => {
  const rn = rowRename(r, 'x', 'a');
  assert.ok(!rn.fields.has('x'));
  assert.ok(rn.fields.has('a'));
});
test('merge: combine', () => {
  const r1 = new TRow({ a: 'Int' }), r2 = new TRow({ b: 'Bool' });
  assert.equal(rowMerge(r1, r2).fields.size, 2);
});
test('merge: duplicate → error', () => {
  assert.throws(() => rowMerge(new TRow({ a: 'Int' }), new TRow({ a: 'Bool' })), /Duplicate/);
});
test('diff: r1 \\ r2', () => {
  const d = rowDiff(r, new TRow({ x: 'Int', y: 'Bool' }));
  assert.equal(d.fields.size, 1);
  assert.ok(d.fields.has('z'));
});
test('intersect: r1 ∩ r2', () => {
  const i = rowIntersect(r, new TRow({ x: 'Int', w: 'Char' }));
  assert.equal(i.fields.size, 1);
  assert.ok(i.fields.has('x'));
});
test('extend + remove = identity', () => {
  const extended = rowExtend(r, 'w', 'Char');
  const back = rowRemove(extended, 'w');
  assert.equal(back.fields.size, r.fields.size);
});

console.log(`\nRow operations tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
