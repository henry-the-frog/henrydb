import { strict as assert } from 'assert';
import {
  Var, Lam, App, Num, Prim, Let,
  ClosureConverter, freeVars, evalCC
} from './closure-convert.js';

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

test('freeVars: variable', () => {
  assert.deepStrictEqual(freeVars(new Var('x')), new Set(['x']));
});

test('freeVars: lambda binds param', () => {
  assert.deepStrictEqual(freeVars(new Lam('x', new Var('x'))), new Set());
});

test('freeVars: lambda with free var', () => {
  const fv = freeVars(new Lam('x', new Prim('+', new Var('x'), new Var('y'))));
  assert.ok(fv.has('y'));
  assert.ok(!fv.has('x'));
});

// ============================================================
// Closure conversion
// ============================================================

test('number: unchanged', () => {
  const cc = new ClosureConverter();
  const { main } = cc.convert(new Num(42));
  assert.equal(main.tag, 'CNum');
  assert.equal(main.n, 42);
});

test('lambda: becomes MakeClosure', () => {
  const cc = new ClosureConverter();
  const { main, topFuns } = cc.convert(new Lam('x', new Var('x')));
  assert.equal(main.tag, 'CMakeClosure');
  assert.equal(topFuns.length, 1);
});

test('app: becomes AppClosure', () => {
  const cc = new ClosureConverter();
  const { main } = cc.convert(new App(new Lam('x', new Var('x')), new Num(5)));
  assert.equal(main.tag, 'CAppClosure');
});

test('closure captures free vars', () => {
  const cc = new ClosureConverter();
  const expr = new Lam('x', new Prim('+', new Var('x'), new Var('y')));
  const { main } = cc.convert(expr);
  // Should capture y
  assert.ok(main.captured.length >= 1);
});

test('multiple lambdas get different labels', () => {
  const cc = new ClosureConverter();
  cc.convert(new Let('f', new Lam('x', new Var('x')),
    new Lam('y', new Var('y'))));
  assert.equal(cc.topFuns.length, 2);
  assert.notEqual(cc.topFuns[0].label, cc.topFuns[1].label);
});

// ============================================================
// Evaluation
// ============================================================

test('eval: (λx.x) 42 → 42', () => {
  const cc = new ClosureConverter();
  const program = cc.convert(new App(new Lam('x', new Var('x')), new Num(42)));
  assert.equal(evalCC(program), 42);
});

test('eval: (λx. x+1) 41 → 42', () => {
  const cc = new ClosureConverter();
  const program = cc.convert(new App(
    new Lam('x', new Prim('+', new Var('x'), new Num(1))),
    new Num(41)));
  assert.equal(evalCC(program), 42);
});

test('eval: closure capture: let y=10 in (λx. x+y) 5 → 15', () => {
  const cc = new ClosureConverter();
  const program = cc.convert(
    new Let('y', new Num(10),
      new App(new Lam('x', new Prim('+', new Var('x'), new Var('y'))), new Num(5))));
  assert.equal(evalCC(program), 15);
});

test('eval: let f = λx. x*2 in f(21) → 42', () => {
  const cc = new ClosureConverter();
  const program = cc.convert(
    new Let('f', new Lam('x', new Prim('*', new Var('x'), new Num(2))),
      new App(new Var('f'), new Num(21))));
  assert.equal(evalCC(program), 42);
});

// ============================================================
// Report
// ============================================================

console.log(`\nClosure conversion tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
