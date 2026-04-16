import { strict as assert } from 'assert';
import { Var, Lam, App, Num, Prim, Let, LambdaLifter, evalLifted, freeVars } from './lambda-lift.js';

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
// Free variables
// ============================================================

test('freeVars: number', () => assert.equal(freeVars(new Num(5)).size, 0));
test('freeVars: variable', () => assert.ok(freeVars(new Var('x')).has('x')));
test('freeVars: lambda binds param', () => {
  assert.equal(freeVars(new Lam(['x'], new Var('x'))).size, 0);
});
test('freeVars: lambda with free var', () => {
  const fv = freeVars(new Lam(['x'], new Prim('+', new Var('x'), new Var('y'))));
  assert.ok(fv.has('y'));
  assert.ok(!fv.has('x'));
});

// ============================================================
// Lambda lifting
// ============================================================

test('number: unchanged', () => {
  const lifter = new LambdaLifter();
  const { main } = lifter.lift(new Num(42));
  assert.equal(main.n, 42);
});

test('closed lambda: becomes top-level', () => {
  const lifter = new LambdaLifter();
  const { main, topDefs } = lifter.lift(new Lam(['x'], new Var('x')));
  assert.equal(topDefs.length, 1);
  assert.equal(topDefs[0].params.length, 1); // Just x, no free vars
});

test('lambda with free var: extra parameter added', () => {
  const lifter = new LambdaLifter();
  const { topDefs } = lifter.lift(new Lam(['x'], new Prim('+', new Var('x'), new Var('y'))));
  assert.equal(topDefs.length, 1);
  assert.ok(topDefs[0].params.includes('y')); // y added as param
  assert.ok(topDefs[0].params.includes('x'));
  assert.ok(topDefs[0].params.length >= 2); // y + x
});

// ============================================================
// Evaluation preserves semantics
// ============================================================

test('eval: (λx.x) 42 → 42', () => {
  const lifter = new LambdaLifter();
  const program = lifter.lift(new App(new Lam(['x'], new Var('x')), [new Num(42)]));
  assert.equal(evalLifted(program), 42);
});

test('eval: (λx. x+1) 41 → 42', () => {
  const lifter = new LambdaLifter();
  const program = lifter.lift(new App(new Lam(['x'], new Prim('+', new Var('x'), new Num(1))), [new Num(41)]));
  assert.equal(evalLifted(program), 42);
});

test('eval: closure: let y=10 in (λx. x+y)(5) → 15', () => {
  const lifter = new LambdaLifter();
  const expr = new Let('y', new Num(10),
    new App(new Lam(['x'], new Prim('+', new Var('x'), new Var('y'))), [new Num(5)]));
  const program = lifter.lift(expr);
  assert.equal(evalLifted(program), 15);
});

test('eval: let binding: let f = λx. x*2 in f(21) → 42', () => {
  const lifter = new LambdaLifter();
  const expr = new Let('f', new Lam(['x'], new Prim('*', new Var('x'), new Num(2))),
    new App(new Var('f'), [new Num(21)]));
  const program = lifter.lift(expr);
  assert.equal(evalLifted(program), 42);
});

// ============================================================
// Report
// ============================================================

console.log(`\nLambda lifting tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
