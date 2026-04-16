import { strict as assert } from 'assert';
import {
  Num, Bool, Str, Fn, Cont,
  evaluate, Env,
  num, bool, str, evar, lam, app, elet, eif, op, reset, shift,
  abort, yieldVal
} from './delimited.js';

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
// Basic evaluation
// ============================================================

test('literal', () => assert.deepStrictEqual(evaluate(num(42)), new Num(42)));
test('addition', () => assert.deepStrictEqual(evaluate(op('+', num(2), num(3))), new Num(5)));
test('let binding', () => assert.deepStrictEqual(evaluate(elet('x', num(5), evar('x'))), new Num(5)));
test('lambda + application', () => {
  assert.deepStrictEqual(evaluate(app(lam('x', op('+', evar('x'), num(1))), num(41))), new Num(42));
});
test('if true', () => assert.deepStrictEqual(evaluate(eif(bool(true), num(1), num(2))), new Num(1)));
test('if false', () => assert.deepStrictEqual(evaluate(eif(bool(false), num(1), num(2))), new Num(2)));

// ============================================================
// reset without shift (trivial)
// ============================================================

test('reset without shift returns normally', () => {
  assert.deepStrictEqual(evaluate(reset(num(42))), new Num(42));
});

test('reset with arithmetic', () => {
  assert.deepStrictEqual(evaluate(reset(op('+', num(1), num(2)))), new Num(3));
});

// ============================================================
// shift/reset: basic examples
// ============================================================

test('shift discards continuation (abort)', () => {
  // reset(1 + shift(k => 42)) = 42 (continuation discarded)
  const expr = reset(op('+', num(1), shift('k', num(42))));
  assert.deepStrictEqual(evaluate(expr), new Num(42));
});

test('shift uses continuation once', () => {
  // reset(1 + shift(k => k(10))) = 1 + 10 = 11
  const expr = reset(op('+', num(1), shift('k', app(evar('k'), num(10)))));
  assert.deepStrictEqual(evaluate(expr), new Num(11));
});

test('shift uses continuation twice', () => {
  // reset(1 + shift(k => k(k(2)))) = 1 + (1 + 2) = 4
  const expr = reset(op('+', num(1), shift('k', app(evar('k'), app(evar('k'), num(2))))));
  assert.deepStrictEqual(evaluate(expr), new Num(4));
});

test('shift returns value directly', () => {
  // reset(shift(k => 100)) = 100
  const expr = reset(shift('k', num(100)));
  assert.deepStrictEqual(evaluate(expr), new Num(100));
});

test('shift with k applied to literal', () => {
  // reset(shift(k => k(5))) = 5
  const expr = reset(shift('k', app(evar('k'), num(5))));
  assert.deepStrictEqual(evaluate(expr), new Num(5));
});

// ============================================================
// Encoding exceptions with shift/reset
// ============================================================

test('abort: exception-like behavior', () => {
  // reset(1 + abort(42)) = 42
  const expr = reset(op('+', num(1), abort(num(42))));
  assert.deepStrictEqual(evaluate(expr), new Num(42));
});

test('abort: deep in expression', () => {
  // reset(1 + (2 * abort(99))) = 99
  const expr = reset(op('+', num(1), op('*', num(2), abort(num(99)))));
  assert.deepStrictEqual(evaluate(expr), new Num(99));
});

// ============================================================
// Encoding state with shift/reset
// ============================================================

test('state-like: accumulate through continuation', () => {
  // Use shift to thread state
  // reset(let x = shift(k => k(5)) in x + 1) = 6
  const expr = reset(elet('x', shift('k', app(evar('k'), num(5))), op('+', evar('x'), num(1))));
  assert.deepStrictEqual(evaluate(expr), new Num(6));
});

// ============================================================
// Nesting
// ============================================================

test('nested resets', () => {
  // reset(1 + reset(2 + shift(k => k(3)))) = 1 + (2 + 3) = 6
  const expr = reset(op('+', num(1), reset(op('+', num(2), shift('k', app(evar('k'), num(3)))))));
  assert.deepStrictEqual(evaluate(expr), new Num(6));
});

test('nested resets with abort in inner', () => {
  // reset(1 + reset(2 + abort(42))) = 1 + 42 = 43
  const expr = reset(op('+', num(1), reset(op('+', num(2), abort(num(42))))));
  assert.deepStrictEqual(evaluate(expr), new Num(43));
});

// ============================================================
// First-class continuations
// ============================================================

test('continuation as function value', () => {
  // reset(let f = shift(k => k) in f(10)) = 10
  const expr = reset(elet('f', shift('k', evar('k')), app(evar('f'), num(10))));
  assert.deepStrictEqual(evaluate(expr), new Num(10));
});

test('continuation stored and called later', () => {
  // reset(shift(k => let x = k(1) in k(2))) 
  // k represents "the rest up to reset"
  // k(1) = 1, k(2) = 2, result depends on last return
  const expr = reset(shift('k', elet('_', app(evar('k'), num(1)), app(evar('k'), num(2)))));
  assert.deepStrictEqual(evaluate(expr), new Num(2));
});

// ============================================================
// Multiplication computation
// ============================================================

test('shift in multiplication', () => {
  // reset(2 * shift(k => k(3) + k(5))) = (2*3) + (2*5) = 16
  const expr = reset(op('*', num(2), shift('k', 
    op('+', app(evar('k'), num(3)), app(evar('k'), num(5))))));
  assert.deepStrictEqual(evaluate(expr), new Num(16));
});

// ============================================================
// Report
// ============================================================

console.log(`\nDelimited continuation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
