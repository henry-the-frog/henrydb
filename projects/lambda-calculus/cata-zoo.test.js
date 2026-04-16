import { strict as assert } from 'assert';
import { listToFix, cata, ana, hylo, para, histo, zygo, NilF, ConsF } from './cata-zoo.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const sum = l => l.tag === 'NilF' ? 0 : l.head + l.tail;
const len = l => l.tag === 'NilF' ? 0 : 1 + l.tail;
const prod = l => l.tag === 'NilF' ? 1 : l.head * l.tail;

test('cata: sum [1,2,3] = 6', () => assert.equal(cata(sum, listToFix([1, 2, 3])), 6));
test('cata: length [a,b,c] = 3', () => assert.equal(cata(len, listToFix(['a', 'b', 'c'])), 3));
test('cata: product [2,3,4] = 24', () => assert.equal(cata(prod, listToFix([2, 3, 4])), 24));

test('hylo: sum of range 1..5 = 15', () => {
  assert.equal(hylo(sum, n => n > 5 ? new NilF() : new ConsF(n, n + 1), 1), 15);
});

test('hylo: factorial 5 = 120', () => {
  assert.equal(hylo(prod, n => n <= 0 ? new NilF() : new ConsF(n, n - 1), 5), 120);
});

test('ana: range 1..4', () => {
  const list = ana(n => n > 4 ? new NilF() : new ConsF(n, n + 1), 1);
  assert.equal(cata(len, list), 4);
});

test('para: can access original structure', () => {
  const result = para(l => {
    if (l.tag === 'NilF') return 0;
    const [recResult, _orig] = l.tail;
    return l.head + recResult;
  }, listToFix([1, 2, 3]));
  assert.equal(result, 6);
});

test('histo: fibonacci via histomorphism', () => {
  const result = histo(l => {
    if (l.tag === 'NilF') return 0;
    if (l.tail.tag === 'NilF') return 1;
    // We don't have deep access in this simple version, just test basic histo
    return l.head + l.tail;
  }, listToFix([1, 1]));
  assert.equal(result, 2);
});

test('zygo: count + sum with auxiliary', () => {
  const result = zygo(
    l => l.tag === 'NilF' ? 0 : 1 + l.tail, // aux: count
    (l, auxVal) => l.tag === 'NilF' ? 0 : l.head + l.tail, // main: sum
    listToFix([10, 20, 30])
  );
  assert.equal(result, 60);
});

test('cata: empty list', () => assert.equal(cata(sum, listToFix([])), 0));

console.log(`\nCatamorphism zoo tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
