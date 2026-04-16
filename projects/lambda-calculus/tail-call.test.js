import { strict as assert } from 'assert';
import { Var, Lam, App, Num, Prim, If, Let, TailCallAnalyzer, canTCO } from './tail-call.js';

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
// Tail position detection
// ============================================================

test('simple tail call: f(x)', () => {
  const analyzer = new TailCallAnalyzer();
  const result = analyzer.analyze(new App(new Var('f'), new Var('x')));
  assert.equal(result.tailCalls.length, 1);
  assert.equal(result.tailCalls[0].callee, 'f');
});

test('non-tail: f(x) + 1', () => {
  const analyzer = new TailCallAnalyzer();
  const result = analyzer.analyze(new Prim('+', new App(new Var('f'), new Var('x')), new Num(1)));
  assert.equal(result.nonTailCalls.length, 1);
  assert.equal(result.tailCalls.length, 0);
});

test('if branches: both tail', () => {
  const analyzer = new TailCallAnalyzer();
  const expr = new If(new Var('cond'),
    new App(new Var('f'), new Var('x')),
    new App(new Var('g'), new Var('y')));
  const result = analyzer.analyze(expr);
  assert.equal(result.tailCalls.length, 2);
});

test('if condition: not tail', () => {
  const analyzer = new TailCallAnalyzer();
  const expr = new If(new App(new Var('pred'), new Var('x')),
    new Num(1), new Num(2));
  const result = analyzer.analyze(expr);
  assert.equal(result.nonTailCalls.length, 1);
});

test('let body: tail', () => {
  const analyzer = new TailCallAnalyzer();
  const expr = new Let('x', new Num(5), new App(new Var('f'), new Var('x')));
  const result = analyzer.analyze(expr);
  assert.equal(result.tailCalls.length, 1);
});

test('let value: not tail', () => {
  const analyzer = new TailCallAnalyzer();
  const expr = new Let('x', new App(new Var('f'), new Num(5)), new Var('x'));
  const result = analyzer.analyze(expr);
  assert.equal(result.nonTailCalls.length, 1);
});

// ============================================================
// Self-recursive tail calls
// ============================================================

test('self-recursive tail call: fact', () => {
  // fact(n) = if n==0 then 1 else n * fact(n-1)
  // The recursive call is NOT in tail position (multiplied by n)
  const body = new If(
    new Prim('==', new Var('n'), new Num(0)),
    new Num(1),
    new Prim('*', new Var('n'), new App(new Var('fact'), new Prim('-', new Var('n'), new Num(1)))));
  
  const result = canTCO('fact', body);
  assert.ok(!result.canOptimize); // Cannot TCO: recursive call not in tail position
  assert.equal(result.nonTailSelfCalls, 1);
});

test('tail-recursive accumulator: fact_acc', () => {
  // fact_acc(n, acc) = if n==0 then acc else fact_acc(n-1, n*acc)
  // The recursive call IS in tail position
  const body = new If(
    new Prim('==', new Var('n'), new Num(0)),
    new Var('acc'),
    new App(new App(new Var('fact_acc'), new Prim('-', new Var('n'), new Num(1))),
      new Prim('*', new Var('n'), new Var('acc'))));
  
  const result = canTCO('fact_acc', body);
  // The outer App is tail, inner App to build curried call is not
  // But the self-referencing call should be detected
  assert.ok(result.selfCalls > 0);
});

test('no self calls: not TCO-able', () => {
  const body = new App(new Var('other'), new Var('x'));
  const result = canTCO('myFn', body);
  assert.ok(!result.canOptimize);
  assert.equal(result.selfCalls, 0);
});

// ============================================================
// Nested functions
// ============================================================

test('lambda body is tail position', () => {
  const expr = new Lam('myFn', 'x', new App(new Var('f'), new Var('x')));
  const analyzer = new TailCallAnalyzer();
  const result = analyzer.analyze(expr);
  assert.equal(result.tailCalls.length, 1);
});

// ============================================================
// Report
// ============================================================

console.log(`\nTail call analysis tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
