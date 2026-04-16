import { strict as assert } from 'assert';
import { tNil, tList, head, tail, length, concat, reverse, map, filter, zip, flatten, includes, unique } from './type-level-lists.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('head: [A,B,C] → A', () => assert.equal(head(tList('A', 'B', 'C')), 'A'));
test('head: [] → null', () => assert.equal(head(tNil), null));
test('tail: [A,B,C] → [B,C]', () => assert.equal(length(tail(tList('A', 'B', 'C'))), 2));
test('length: [A,B,C] = 3', () => assert.equal(length(tList('A', 'B', 'C')), 3));
test('length: [] = 0', () => assert.equal(length(tNil), 0));
test('concat: [A]+[B,C] = [A,B,C]', () => assert.equal(length(concat(tList('A'), tList('B', 'C'))), 3));
test('reverse: [1,2,3] → [3,2,1]', () => {
  const r = reverse(tList(1, 2, 3));
  assert.equal(head(r), 3);
});
test('map: double [1,2,3]', () => {
  const r = map(x => x * 2, tList(1, 2, 3));
  assert.equal(head(r), 2);
  assert.equal(length(r), 3);
});
test('filter: evens from [1,2,3,4]', () => {
  const r = filter(x => x % 2 === 0, tList(1, 2, 3, 4));
  assert.equal(length(r), 2);
});
test('zip: [1,2]+[a,b] → [[1,a],[2,b]]', () => {
  const r = zip(tList(1, 2), tList('a', 'b'));
  assert.equal(length(r), 2);
});
test('flatten: [[1,2],[3]] → [1,2,3]', () => {
  const r = flatten(tList(tList(1, 2), tList(3)));
  assert.equal(length(r), 3);
});
test('includes: [1,2,3] has 2', () => assert.ok(includes(tList(1, 2, 3), 2)));
test('unique: [1,1,2,2,3] → [1,2,3]', () => assert.equal(length(unique(tList(1, 1, 2, 2, 3))), 3));

console.log(`\nType-level lists tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
