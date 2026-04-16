import { strict as assert } from 'assert';
import { Datatype, Ornament, forget, Nat, List, listToNat, length, natToNum, mkList, mkNat } from './ornaments.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('mkList: empty', () => assert.equal(mkList().tag, 'Nil'));
test('mkList: [1,2,3]', () => assert.equal(mkList(1, 2, 3).head, 1));
test('mkNat: 0', () => assert.equal(mkNat(0).tag, 'Zero'));
test('mkNat: 3', () => assert.equal(natToNum(mkNat(3)), 3));

test('forget: Nil → Zero', () => {
  const r = length(mkList());
  assert.equal(r.tag, 'Zero');
});

test('forget: [a,b,c] → Succ(Succ(Succ(Zero)))', () => {
  const r = length(mkList('a', 'b', 'c'));
  assert.equal(r.tag, 'Succ');
});

test('Nat datatype has 2 constructors', () => assert.equal(Nat.constructors.size, 2));
test('List datatype has 2 constructors', () => assert.equal(List.constructors.size, 2));

test('ornament mapping: Nil → Zero', () => {
  assert.equal(listToNat.mapping.get('Nil').from, 'Zero');
});

test('ornament mapping: Cons → Succ', () => {
  assert.equal(listToNat.mapping.get('Cons').from, 'Succ');
  assert.deepStrictEqual(listToNat.mapping.get('Cons').addedFields, ['head']);
});

console.log(`\nOrnaments tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
