import { strict as assert } from 'assert';
import { Signature, Module, Functor, ComparableSig, IntComparable, StrComparable, MakeSortedSet } from './first-class-modules.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('signature: IntComparable satisfies Comparable', () => {
  assert.ok(ComparableSig.satisfiedBy(IntComparable).ok);
});

test('signature: incomplete module fails', () => {
  const bad = new Module('Bad', new Map(), new Map());
  assert.ok(!ComparableSig.satisfiedBy(bad).ok);
});

test('module: getValue', () => {
  const cmp = IntComparable.getValue('compare');
  assert.equal(cmp(1, 2), -1);
});

test('functor: apply creates SortedSet', () => {
  const IntSet = MakeSortedSet.apply(IntComparable);
  assert.ok(IntSet.getValue('empty'));
  assert.ok(IntSet.getValue('insert'));
});

test('SortedSet: insert and contains', () => {
  const S = MakeSortedSet.apply(IntComparable);
  let set = S.getValue('empty')();
  set = S.getValue('insert')(3, set);
  set = S.getValue('insert')(1, set);
  set = S.getValue('insert')(2, set);
  assert.ok(S.getValue('contains')(2, set));
  assert.ok(!S.getValue('contains')(4, set));
});

test('SortedSet: maintains order', () => {
  const S = MakeSortedSet.apply(IntComparable);
  let set = S.getValue('empty')();
  set = S.getValue('insert')(3, set);
  set = S.getValue('insert')(1, set);
  set = S.getValue('insert')(2, set);
  assert.deepStrictEqual(S.getValue('toList')(set), [1, 2, 3]);
});

test('SortedSet: no duplicates', () => {
  const S = MakeSortedSet.apply(IntComparable);
  let set = S.getValue('empty')();
  set = S.getValue('insert')(1, set);
  set = S.getValue('insert')(1, set);
  assert.equal(S.getValue('size')(set), 1);
});

test('functor: works with StrComparable too', () => {
  const StrSet = MakeSortedSet.apply(StrComparable);
  let set = StrSet.getValue('empty')();
  set = StrSet.getValue('insert')('c', set);
  set = StrSet.getValue('insert')('a', set);
  assert.deepStrictEqual(StrSet.getValue('toList')(set), ['a', 'c']);
});

test('functor: rejects invalid module', () => {
  assert.throws(() => MakeSortedSet.apply(new Module('Bad', new Map(), new Map())), /Missing/);
});

test('module: extend', () => {
  const ext = IntComparable.extend('Extended', new Map(), new Map([['show', x => String(x)]]));
  assert.ok(ext.getValue('compare'));
  assert.ok(ext.getValue('show'));
});

console.log(`\nFirst-class modules tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
