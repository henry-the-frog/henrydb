import { strict as assert } from 'assert';
import { CEKMachine, v, lam, app, n, prim, if0, let_, NumVal, Closure } from './cek.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

function evalCEK(expr, opts) {
  const machine = new CEKMachine(opts);
  return machine.eval(expr);
}

// ============================================================
// Basic evaluation
// ============================================================

test('number', () => {
  const { value } = evalCEK(n(42));
  assert.equal(value.n, 42);
});

test('identity: (λx.x) 5 → 5', () => {
  const { value } = evalCEK(app(lam('x', v('x')), n(5)));
  assert.equal(value.n, 5);
});

test('constant: (λx.42) 99 → 42', () => {
  const { value } = evalCEK(app(lam('x', n(42)), n(99)));
  assert.equal(value.n, 42);
});

test('lambda returns closure', () => {
  const { value } = evalCEK(lam('x', v('x')));
  assert.equal(value.tag, 'Closure');
  assert.equal(value.param, 'x');
});

// ============================================================
// Arithmetic
// ============================================================

test('addition: 3 + 4 → 7', () => {
  const { value } = evalCEK(prim('+', n(3), n(4)));
  assert.equal(value.n, 7);
});

test('subtraction: 10 - 3 → 7', () => {
  const { value } = evalCEK(prim('-', n(10), n(3)));
  assert.equal(value.n, 7);
});

test('multiplication: 6 * 7 → 42', () => {
  const { value } = evalCEK(prim('*', n(6), n(7)));
  assert.equal(value.n, 42);
});

test('nested arithmetic: (2 + 3) * (4 - 1) → 15', () => {
  const { value } = evalCEK(prim('*', prim('+', n(2), n(3)), prim('-', n(4), n(1))));
  assert.equal(value.n, 15);
});

// ============================================================
// If0
// ============================================================

test('if0 true: if0(0, 1, 2) → 1', () => {
  const { value } = evalCEK(if0(n(0), n(1), n(2)));
  assert.equal(value.n, 1);
});

test('if0 false: if0(1, 1, 2) → 2', () => {
  const { value } = evalCEK(if0(n(1), n(1), n(2)));
  assert.equal(value.n, 2);
});

// ============================================================
// Let binding
// ============================================================

test('let: let x = 5 in x + 1 → 6', () => {
  const { value } = evalCEK(let_('x', n(5), prim('+', v('x'), n(1))));
  assert.equal(value.n, 6);
});

test('nested let: let x = 2 in let y = 3 in x * y → 6', () => {
  const { value } = evalCEK(let_('x', n(2), let_('y', n(3), prim('*', v('x'), v('y')))));
  assert.equal(value.n, 6);
});

// ============================================================
// Higher-order functions
// ============================================================

test('apply twice: (λf.λx. f(f(x))) (λy. y+1) 3 → 5', () => {
  const double = lam('f', lam('x', app(v('f'), app(v('f'), v('x')))));
  const inc = lam('y', prim('+', v('y'), n(1)));
  const { value } = evalCEK(app(app(double, inc), n(3)));
  assert.equal(value.n, 5);
});

test('church encoding: succ 0 → 1', () => {
  // succ = λn.λf.λx. f (n f x)
  // zero = λf.λx. x
  // toNum = λn. n (λx. x+1) 0
  const zero = lam('f', lam('x', v('x')));
  const succ = lam('n', lam('f', lam('x', app(v('f'), app(app(v('n'), v('f')), v('x'))))));
  const toNum = lam('n', app(app(v('n'), lam('x', prim('+', v('x'), n(1)))), n(0)));
  
  const { value } = evalCEK(app(toNum, app(succ, zero)));
  assert.equal(value.n, 1);
});

// ============================================================
// Closures capture environment
// ============================================================

test('closure captures environment', () => {
  // let a = 10 in let f = λx. x + a in f(5) → 15
  const expr = let_('a', n(10), let_('f', lam('x', prim('+', v('x'), v('a'))), app(v('f'), n(5))));
  const { value } = evalCEK(expr);
  assert.equal(value.n, 15);
});

test('closure with shadowing', () => {
  // let x = 1 in let f = λx. x + 10 in f(2) → 12
  const expr = let_('x', n(1), let_('f', lam('x', prim('+', v('x'), n(10))), app(v('f'), n(2))));
  const { value } = evalCEK(expr);
  assert.equal(value.n, 12);
});

// ============================================================
// Recursion via Y-like pattern
// ============================================================

test('factorial via self-application', () => {
  // fact_step = λself.λn. if0(n, 1, n * self(self)(n-1))
  // fact = fact_step(fact_step)
  const factStep = lam('self', lam('n',
    if0(v('n'), n(1),
      prim('*', v('n'), app(app(v('self'), v('self')), prim('-', v('n'), n(1)))))));
  const fact = app(factStep, factStep);
  const { value } = evalCEK(app(fact, n(5)));
  assert.equal(value.n, 120);
});

// ============================================================
// Step counting
// ============================================================

test('step count: identity takes few steps', () => {
  const { steps } = evalCEK(app(lam('x', v('x')), n(5)));
  assert.ok(steps < 10);
});

test('step count: arithmetic takes predictable steps', () => {
  const { steps } = evalCEK(prim('+', n(1), n(2)));
  assert.ok(steps > 0 && steps < 20);
});

// ============================================================
// Error handling
// ============================================================

test('unbound variable throws', () => {
  assert.throws(() => evalCEK(v('undefined_var')), /Unbound/);
});

test('apply non-function throws', () => {
  assert.throws(() => evalCEK(app(n(5), n(3))), /not a function/);
});

// ============================================================
// Tracing
// ============================================================

test('trace captures steps', () => {
  const machine = new CEKMachine({ trace: true });
  machine.eval(app(lam('x', v('x')), n(42)));
  assert.ok(machine.steps.length > 0);
  assert.ok(machine.steps[0].step === 0);
});

// ============================================================
// Report
// ============================================================

console.log(`\nCEK machine tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
