import { strict as assert } from 'assert';
import { Var, Abs, App, parse, prettyPrint } from './lambda.js';
import { Tracer, compareStrategies, churchNum, isChurchNum } from './small-step.js';

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
// Basic tracing
// ============================================================

test('identity function: (λx.x) y → y', () => {
  const tracer = new Tracer();
  const expr = new App(new Abs('x', new Var('x')), new Var('y'));
  const trace = tracer.trace(expr);
  assert.equal(trace.stepCount, 1);
  assert.ok(!trace.diverged);
  assert.equal(prettyPrint(trace.normalForm), 'y');
});

test('already in normal form: variable', () => {
  const tracer = new Tracer();
  const trace = tracer.trace(new Var('x'));
  assert.equal(trace.stepCount, 0);
  assert.equal(prettyPrint(trace.normalForm), 'x');
});

test('already in normal form: abstraction', () => {
  const tracer = new Tracer();
  const trace = tracer.trace(new Abs('x', new Var('x')));
  assert.equal(trace.stepCount, 0);
});

test('two-step reduction', () => {
  // (λx.x) ((λy.y) z) → (λx.x) z → z (normal order)
  const expr = new App(
    new Abs('x', new Var('x')),
    new App(new Abs('y', new Var('y')), new Var('z'))
  );
  const tracer = new Tracer({ strategy: 'normal' });
  const trace = tracer.trace(expr);
  assert.equal(trace.stepCount, 2);
  assert.equal(prettyPrint(trace.normalForm), 'z');
});

// ============================================================
// Church numerals
// ============================================================

test('church numeral: 0', () => {
  const zero = churchNum(0);
  assert.equal(isChurchNum(zero), 0);
});

test('church numeral: 3', () => {
  const three = churchNum(3);
  assert.equal(isChurchNum(three), 3);
});

test('church successor', () => {
  const tracer = new Tracer({ maxSteps: 50 });
  // SUCC = λn.λf.λx. f (n f x)
  const succ = parse('(λn.λf.λx. f (n f x))');
  const zero = churchNum(0);
  const trace = tracer.trace(new App(succ, zero));
  assert.ok(!trace.diverged);
  // Result should be church numeral 1
  const result = isChurchNum(trace.normalForm);
  assert.equal(result, 1);
});

// ============================================================
// Divergence detection
// ============================================================

test('omega diverges', () => {
  // Ω = (λx. x x)(λx. x x)
  const omega = new App(
    new Abs('x', new App(new Var('x'), new Var('x'))),
    new Abs('x', new App(new Var('x'), new Var('x')))
  );
  const tracer = new Tracer({ maxSteps: 10 });
  const trace = tracer.trace(omega);
  assert.ok(trace.diverged);
});

test('countSteps: divergent returns -1', () => {
  const omega = new App(
    new Abs('x', new App(new Var('x'), new Var('x'))),
    new Abs('x', new App(new Var('x'), new Var('x')))
  );
  const tracer = new Tracer({ maxSteps: 10 });
  assert.equal(tracer.countSteps(omega), -1);
});

// ============================================================
// Strategy comparison
// ============================================================

test('normal vs applicative: identity applied to value', () => {
  const expr = new App(new Abs('x', new Var('x')), new Var('y'));
  const results = compareStrategies(expr);
  // Both should reach normal form in 1 step
  assert.equal(results.normal.steps, 1);
  assert.equal(results.applicative.steps, 1);
});

test('normal order can find normal form when applicative diverges', () => {
  // (λx.λy. x) z Ω — normal order reduces to z, applicative tries to evaluate Ω first
  const omega = new App(
    new Abs('x', new App(new Var('x'), new Var('x'))),
    new Abs('x', new App(new Var('x'), new Var('x')))
  );
  const expr = new App(new App(new Abs('x', new Abs('y', new Var('x'))), new Var('z')), omega);
  
  const normalTracer = new Tracer({ strategy: 'normal', maxSteps: 20 });
  const normalTrace = normalTracer.trace(expr);
  assert.ok(!normalTrace.diverged);
  
  // Applicative order tries to evaluate omega first
  const appTracer = new Tracer({ strategy: 'applicative', maxSteps: 20 });
  const appTrace = appTracer.trace(expr);
  assert.ok(appTrace.diverged);
});

// ============================================================
// Trace formatting
// ============================================================

test('formatTrace produces readable output', () => {
  const tracer = new Tracer();
  const expr = new App(new Abs('x', new Var('x')), new Var('y'));
  const trace = tracer.trace(expr);
  const formatted = tracer.formatTrace(trace);
  assert.ok(formatted.length >= 2);
  assert.ok(formatted[0].includes('λ') || formatted[0].includes('x'));
});

test('trace steps have rule annotations', () => {
  const tracer = new Tracer();
  const expr = new App(new Abs('x', new Var('x')), new Var('y'));
  const trace = tracer.trace(expr);
  // Second step should have a β rule
  if (trace.steps.length > 1) {
    assert.ok(trace.steps[1].rule.includes('β'));
  }
});

// ============================================================
// Complex: boolean operations
// ============================================================

test('boolean TRUE/FALSE via church encoding', () => {
  // TRUE = λt.λf. t, FALSE = λt.λf. f
  // NOT = λb.λt.λf. b f t
  const TRUE = new Abs('t', new Abs('f', new Var('t')));
  const NOT = parse('(λb.λt.λf. b f t)');
  
  const tracer = new Tracer({ maxSteps: 20 });
  const trace = tracer.trace(new App(NOT, TRUE));
  assert.ok(!trace.diverged);
  // NOT TRUE = FALSE = λt.λf. f
  const result = trace.normalForm;
  assert.ok(result instanceof Abs);
  assert.ok(result.body instanceof Abs);
  assert.ok(result.body.body instanceof Var);
  assert.equal(result.body.body.name, result.body.param); // Returns the second arg = false
});

// ============================================================
// Report
// ============================================================

console.log(`\nSmall-step semantics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
