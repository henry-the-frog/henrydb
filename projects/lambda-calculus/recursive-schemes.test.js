import { strict as assert } from 'assert';
import { NilF, ConsF, cata, ana, hylo, para, apo, Left, Right, listToFix, fixToList } from './recursive-schemes.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('cata: sum [1,2,3] = 6', () => {
  assert.equal(cata(l => l.tag === 'NilF' ? 0 : l.head + l.tail, listToFix([1, 2, 3])), 6);
});

test('cata: length [a,b,c] = 3', () => {
  assert.equal(cata(l => l.tag === 'NilF' ? 0 : 1 + l.tail, listToFix(['a', 'b', 'c'])), 3);
});

test('ana: range 1..5', () => {
  const list = ana(n => n > 5 ? new NilF() : new ConsF(n, n + 1), 1);
  assert.deepStrictEqual(fixToList(list), [1, 2, 3, 4, 5]);
});

test('hylo: sum of range (no intermediate list)', () => {
  const result = hylo(
    l => l.tag === 'NilF' ? 0 : l.head + l.tail,
    n => n > 5 ? new NilF() : new ConsF(n, n + 1),
    1
  );
  assert.equal(result, 15);
});

test('para: tails [1,2,3]', () => {
  const result = para(l => {
    if (l.tag === 'NilF') return [[]];
    const [recResult, originalTail] = l.tail;
    return [fixToList(originalTail), ...recResult];
  }, listToFix([1, 2, 3]));
  assert.equal(result.length, 4); // [[2,3], [3], [], []]
});

test('apo: take 3 from infinite-like', () => {
  const list = apo(n => {
    if (n >= 3) return new ConsF(n, Left(listToFix([]))); // Stop
    return new ConsF(n, Right(n + 1)); // Continue
  }, 0);
  assert.deepStrictEqual(fixToList(list), [0, 1, 2, 3]);
});

test('listToFix/fixToList roundtrip', () => {
  assert.deepStrictEqual(fixToList(listToFix([10, 20, 30])), [10, 20, 30]);
});

test('cata: product', () => {
  assert.equal(cata(l => l.tag === 'NilF' ? 1 : l.head * l.tail, listToFix([2, 3, 4])), 24);
});

test('hylo: factorial', () => {
  const result = hylo(
    l => l.tag === 'NilF' ? 1 : l.head * l.tail,
    n => n <= 0 ? new NilF() : new ConsF(n, n - 1),
    5
  );
  assert.equal(result, 120);
});

test('ana: empty', () => assert.deepStrictEqual(fixToList(ana(n => new NilF(), 0)), []));

console.log(`\nRecursive schemes tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
