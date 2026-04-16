import { strict as assert } from 'assert';
import { Thunk, thunk, force, LazyList, lazyRange, lazyFrom, isWHNF } from './lazy-eval.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Thunks
test('thunk: lazy evaluation', () => {
  let evaluated = false;
  const t = thunk(() => { evaluated = true; return 42; });
  assert.ok(!evaluated);
  assert.equal(force(t), 42);
  assert.ok(evaluated);
});

test('thunk: memoization (sharing)', () => {
  let count = 0;
  const t = thunk(() => { count++; return 42; });
  force(t); force(t); force(t);
  assert.equal(count, 1); // Computed only once!
});

test('thunk: infinite loop detection', () => {
  const t = thunk(() => force(t)); // Self-referential
  assert.throws(() => force(t), /Infinite loop/);
});

test('thunk: isEvaluated', () => {
  const t = thunk(() => 42);
  assert.ok(!t.isEvaluated);
  force(t);
  assert.ok(t.isEvaluated);
});

// Lazy lists
test('lazyRange: take 5 from [1,2,3,...]', () => {
  assert.deepStrictEqual(lazyRange(1).take(5), [1, 2, 3, 4, 5]);
});

test('lazyRange: take 0', () => {
  assert.deepStrictEqual(lazyRange(1).take(0), []);
});

test('lazyList: map', () => {
  const doubled = lazyRange(1).map(x => x * 2);
  assert.deepStrictEqual(doubled.take(4), [2, 4, 6, 8]);
});

test('lazyList: filter', () => {
  const evens = lazyRange(1).filter(x => x % 2 === 0);
  assert.deepStrictEqual(evens.take(3), [2, 4, 6]);
});

test('lazyFrom: finite list', () => {
  const l = lazyFrom([10, 20, 30]);
  assert.deepStrictEqual(l.take(3), [10, 20, 30]);
});

// WHNF
test('isWHNF: evaluated thunk', () => {
  const t = thunk(() => 42);
  assert.ok(!isWHNF(t));
  force(t);
  assert.ok(isWHNF(t));
});

test('isWHNF: primitive always in WHNF', () => {
  assert.ok(isWHNF(42));
  assert.ok(isWHNF('hello'));
});

// Sieve of Eratosthenes (classic lazy example)
test('sieve: first 5 primes', () => {
  function sieve(list) {
    const p = list.head;
    return new LazyList(
      thunk(() => p),
      thunk(() => sieve(list.tail.filter(x => x % p !== 0)))
    );
  }
  const primes = sieve(lazyRange(2));
  assert.deepStrictEqual(primes.take(5), [2, 3, 5, 7, 11]);
});

console.log(`\nLazy evaluation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
