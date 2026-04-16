import { strict as assert } from 'assert';
import {
  ExceptionMonad, StateMonad, NondeterminismMonad, LoggerMonad,
  AlgEffects, DelimitedConts,
  Programs
} from './effects-rosetta.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Exception: all 3 agree
// ============================================================

test('exception monad: safe division success', () => {
  const result = Programs.safeDivision.monadic(10, 3);
  assert.deepStrictEqual(result, { tag: 'Ok', value: 3 });
});

test('exception monad: division by zero', () => {
  const result = Programs.safeDivision.monadic(10, 0);
  assert.deepStrictEqual(result, { tag: 'Err', error: 'division by zero' });
});

test('exception alg effects: success', () => {
  const result = Programs.safeDivision.algebraic(10, 3);
  assert.equal(result, 3);
});

test('exception alg effects: division by zero', () => {
  const result = Programs.safeDivision.algebraic(10, 0);
  assert.deepStrictEqual(result, { tag: 'Err', error: 'division by zero' });
});

test('exception delimited: success', () => {
  const result = Programs.safeDivision.delimited(10, 3);
  assert.deepStrictEqual(result, { tag: 'Ok', value: 3 });
});

test('exception delimited: division by zero', () => {
  const result = Programs.safeDivision.delimited(10, 0);
  assert.deepStrictEqual(result, { tag: 'Err', error: 'division by zero' });
});

// ============================================================
// State: all 3 agree
// ============================================================

test('state monad: counter to 3', () => {
  const result = Programs.counter.monadic();
  assert.equal(result.value, 3);
  assert.equal(result.state, 3);
});

test('state alg effects: counter to 3', () => {
  const result = Programs.counter.algebraic();
  assert.equal(result.value, 3);
  assert.equal(result.state, 3);
});

test('state delimited: counter to 3', () => {
  const result = Programs.counter.delimited();
  assert.equal(result.value, 3);
  assert.equal(result.state, 3);
});

// ============================================================
// Nondeterminism: all 3 agree
// ============================================================

test('nondeterminism monad: coin flips', () => {
  const result = Programs.coinFlip.monadic();
  assert.deepStrictEqual(result.sort(), ['HH', 'HT', 'TH', 'TT']);
});

test('nondeterminism alg effects: coin flips', () => {
  const result = Programs.coinFlip.algebraic();
  assert.deepStrictEqual(result.sort(), ['HH', 'HT', 'TH', 'TT']);
});

test('nondeterminism delimited: coin flips', () => {
  const result = Programs.coinFlip.delimited();
  assert.deepStrictEqual(result.sort(), ['HH', 'HT', 'TH', 'TT']);
});

// ============================================================
// Logger: all 3 agree
// ============================================================

test('logger monad: logs and value', () => {
  const result = Programs.logging.monadic();
  assert.equal(result.value, 42);
  assert.deepStrictEqual(result.log, ['start', 'computed: 42']);
});

test('logger alg effects: logs and value', () => {
  const result = Programs.logging.algebraic();
  assert.equal(result.value, 42);
  assert.deepStrictEqual(result.log, ['start', 'computed: 42']);
});

test('logger delimited: logs and value', () => {
  const result = Programs.logging.delimited();
  assert.equal(result.value, 42);
  assert.deepStrictEqual(result.log, ['start', 'computed: 42']);
});

// ============================================================
// Cross-cutting: monads compose
// ============================================================

test('exception monad: chained operations', () => {
  const M = ExceptionMonad;
  const result = M.bind(M.return(10), x =>
    M.bind(M.return(x * 2), y =>
      M.return(y + 1)));
  assert.deepStrictEqual(result, { tag: 'Ok', value: 21 });
});

test('exception monad: short-circuit on error', () => {
  const M = ExceptionMonad;
  const result = M.bind(M.throw('oops'), x =>
    M.return(x + 1)); // Never reached
  assert.deepStrictEqual(result, { tag: 'Err', error: 'oops' });
});

test('state monad: multiple get/put', () => {
  const M = StateMonad;
  const prog = M.bind(M.put(10), () =>
    M.bind(M.get(), x =>
      M.bind(M.put(x * 2), () =>
        M.get())));
  const result = M.run(prog, 0);
  assert.equal(result.value, 20);
  assert.equal(result.state, 20);
});

test('nondeterminism monad: filter', () => {
  const M = NondeterminismMonad;
  const result = M.bind(M.choose([1,2,3,4,5]), x =>
    x % 2 === 0 ? M.return(x) : M.fail());
  assert.deepStrictEqual(result, [2, 4]);
});

// ============================================================
// Equivalence verification
// ============================================================

test('ALL exception results agree', () => {
  for (const [a, b] of [[10,3], [10,0], [100,7], [0,5], [1,1]]) {
    const mon = Programs.safeDivision.monadic(a, b);
    const del = Programs.safeDivision.delimited(a, b);
    // Both should have same tag and same value/error
    assert.equal(mon.tag, del.tag);
    if (mon.tag === 'Ok') assert.equal(mon.value, del.value);
    if (mon.tag === 'Err') assert.equal(mon.error, del.error);
  }
});

test('ALL state results agree', () => {
  const mon = Programs.counter.monadic();
  const alg = Programs.counter.algebraic();
  const del = Programs.counter.delimited();
  assert.equal(mon.value, alg.value);
  assert.equal(alg.value, del.value);
  assert.equal(mon.state, alg.state);
  assert.equal(alg.state, del.state);
});

test('ALL nondeterminism results agree', () => {
  const mon = Programs.coinFlip.monadic().sort();
  const alg = Programs.coinFlip.algebraic().sort();
  const del = Programs.coinFlip.delimited().sort();
  assert.deepStrictEqual(mon, alg);
  assert.deepStrictEqual(alg, del);
});

test('ALL logger results agree', () => {
  const mon = Programs.logging.monadic();
  const alg = Programs.logging.algebraic();
  const del = Programs.logging.delimited();
  assert.equal(mon.value, alg.value);
  assert.equal(alg.value, del.value);
  assert.deepStrictEqual(mon.log, alg.log);
  assert.deepStrictEqual(alg.log, del.log);
});

// ============================================================
// Report
// ============================================================

console.log(`\nEffects Rosetta tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
