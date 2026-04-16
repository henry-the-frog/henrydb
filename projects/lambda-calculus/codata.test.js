import { strict as assert } from 'assert';
import {
  cons, repeat, iterate, unfold, nats, fibs,
  take, drop, smap, sfilter, szipWith, stakeWhile, sinterleave,
  bisimilar
} from './codata.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('repeat: infinite stream of 1s', () => {
  assert.deepStrictEqual(take(5, repeat(1)), [1, 1, 1, 1, 1]);
});

test('nats: 0, 1, 2, ...', () => {
  assert.deepStrictEqual(take(5, nats()), [0, 1, 2, 3, 4]);
});

test('iterate: powers of 2', () => {
  assert.deepStrictEqual(take(5, iterate(x => x * 2, 1)), [1, 2, 4, 8, 16]);
});

test('fibs: Fibonacci sequence', () => {
  assert.deepStrictEqual(take(8, fibs()), [0, 1, 1, 2, 3, 5, 8, 13]);
});

test('unfold: counting', () => {
  const stream = unfold(n => [n, n + 1], 0);
  assert.deepStrictEqual(take(4, stream), [0, 1, 2, 3]);
});

test('map: double nats', () => {
  assert.deepStrictEqual(take(5, smap(x => x * 2, nats())), [0, 2, 4, 6, 8]);
});

test('filter: even numbers', () => {
  assert.deepStrictEqual(take(5, sfilter(x => x % 2 === 0, nats())), [0, 2, 4, 6, 8]);
});

test('zipWith: add nats to itself', () => {
  assert.deepStrictEqual(take(5, szipWith((a, b) => a + b, nats(), nats())), [0, 2, 4, 6, 8]);
});

test('drop: skip first 5 nats', () => {
  assert.deepStrictEqual(take(3, drop(5, nats())), [5, 6, 7]);
});

test('takeWhile: nats < 5', () => {
  assert.deepStrictEqual(stakeWhile(x => x < 5, nats()), [0, 1, 2, 3, 4]);
});

test('interleave: odds and evens', () => {
  const evens = sfilter(x => x % 2 === 0, nats());
  const odds = sfilter(x => x % 2 === 1, nats());
  assert.deepStrictEqual(take(6, sinterleave(evens, odds)), [0, 1, 2, 3, 4, 5]);
});

test('bisimilar: nats() ≈ iterate(+1, 0)', () => {
  assert.ok(bisimilar(nats(), iterate(x => x + 1, 0)));
});

test('Fibonacci via zipWith (Haskell-style)', () => {
  // fibs = 0 : 1 : zipWith (+) fibs (tail fibs)
  function haskellFibs() {
    const fibs = cons(0, () => cons(1, () => szipWith((a, b) => a + b, fibs, fibs.tail())));
    return fibs;
  }
  assert.deepStrictEqual(take(8, haskellFibs()), [0, 1, 1, 2, 3, 5, 8, 13]);
});

console.log(`\nCodata tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
