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

test('continuation as function value (returns captured k)', () => {
  // reset(let f = shift(k => k) in f(10))
  // shift captures k = λv. let f = v in f(10), then returns k itself
  // So reset returns the captured continuation (a Cont value)
  const expr = reset(elet('f', shift('k', evar('k')), app(evar('f'), num(10))));
  const result = evaluate(expr);
  assert.equal(result.tag, 'Cont');
  // But we CAN apply the continuation outside: k(id) = let f = id in f(10) = id(10) = 10
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
// Stress tests: multi-shot continuations
// ============================================================

test('multi-shot: k applied 3 times', () => {
  // reset(1 + shift(k => k(1) + k(2) + k(3)))
  // k = λx. 1 + x
  // k(1) + k(2) + k(3) = 2 + 3 + 4 = 9
  const expr = reset(op('+', num(1), shift('k', 
    op('+', op('+', app(evar('k'), num(1)), app(evar('k'), num(2))), app(evar('k'), num(3))))));
  assert.deepStrictEqual(evaluate(expr), new Num(9));
});

test('multi-shot: k in nested arithmetic', () => {
  // reset(3 * shift(k => k(2) * k(4)))
  // k = λx. 3 * x
  // k(2) * k(4) = 6 * 12 = 72
  const expr = reset(op('*', num(3), shift('k',
    op('*', app(evar('k'), num(2)), app(evar('k'), num(4))))));
  assert.deepStrictEqual(evaluate(expr), new Num(72));
});

test('multi-shot: k(k(k(2)))', () => {
  // reset(1 + shift(k => k(k(k(2)))))
  // k = λx. 1 + x
  // k(k(k(2))) = k(k(3)) = k(4) = 5
  const expr = reset(op('+', num(1), shift('k', 
    app(evar('k'), app(evar('k'), app(evar('k'), num(2)))))));
  assert.deepStrictEqual(evaluate(expr), new Num(5));
});

// ============================================================
// Stress tests: nested shift/reset
// ============================================================

test('nested: shift inside shift body', () => {
  // reset(1 + shift(k => reset(2 + shift(j => j(k(10))))))
  // k = λx. 1 + x, j = λx. 2 + x
  // j(k(10)) = j(11) = 13
  const expr = reset(op('+', num(1), shift('k',
    reset(op('+', num(2), shift('j',
      app(evar('j'), app(evar('k'), num(10)))))))));
  assert.deepStrictEqual(evaluate(expr), new Num(13));
});

test('nested: abort in nested reset', () => {
  // reset(10 + reset(20 + shift(k => 99)))
  // Inner reset: 20 + shift(k => 99) → abort with 99
  // Outer: 10 + 99 = 109
  const expr = reset(op('+', num(10), reset(op('+', num(20), shift('_k', num(99))))));
  assert.deepStrictEqual(evaluate(expr), new Num(109));
});

test('double nested resets with continuation passing', () => {
  // reset(reset(shift(k => k(5)) + 1) * 2)
  // Inner shift captures k = λx. x + 1, returns k(5) = 6
  // Inner reset returns 6
  // Outer: 6 * 2 = 12
  const expr = reset(op('*', reset(op('+', shift('k', app(evar('k'), num(5))), num(1))), num(2)));
  assert.deepStrictEqual(evaluate(expr), new Num(12));
});

// ============================================================
// Stress tests: encoding patterns
// ============================================================

test('encoding: nondeterministic choice', () => {
  // Nondeterminism: shift captures "the future" and runs it with each choice
  // reset(let x = shift(k => [k(1), k(2), k(3)]) in x * x)
  // k = λx. x * x → [1, 4, 9]
  // But our evaluator returns the list value from shift body
  // We need to check the shift body result
  const expr = reset(
    elet('x', shift('k', 
      // Can't build list in our language, so simulate with arithmetic
      // k(1) + k(2) + k(3)
      op('+', op('+', app(evar('k'), num(1)), app(evar('k'), num(2))), app(evar('k'), num(3)))
    ), op('*', evar('x'), evar('x')))
  );
  // k(1) + k(2) + k(3) = 1 + 4 + 9 = 14
  assert.deepStrictEqual(evaluate(expr), new Num(14));
});

test('encoding: state passing', () => {
  // Simulate get/put with shift/reset
  // reset(let x = shift(k => k(42)) in x + x)
  // k = λv. v + v, k(42) = 84
  const expr = reset(elet('x', shift('k', app(evar('k'), num(42))), op('+', evar('x'), evar('x'))));
  assert.deepStrictEqual(evaluate(expr), new Num(84));
});

test('encoding: exception with handler', () => {
  // reset(1 + (2 * shift(k => str("error")))) = "error"
  const expr = reset(op('+', num(1), op('*', num(2), shift('_k', str('error')))));
  assert.deepStrictEqual(evaluate(expr), new Str('error'));
});

// ============================================================
// Edge cases
// ============================================================

test('shift with identity continuation application', () => {
  // reset(shift(k => k(42)))  → k = identity, k(42) = 42
  const expr = reset(shift('k', app(evar('k'), num(42))));
  assert.deepStrictEqual(evaluate(expr), new Num(42));
});

test('reset around pure value with outer computation', () => {
  // 10 + reset(20) = 30
  const expr = op('+', num(10), reset(num(20)));
  assert.deepStrictEqual(evaluate(expr), new Num(30));
});

// ============================================================
// Report
// ============================================================

console.log(`\nDelimited continuation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
