import { strict as assert } from 'assert';
import {
  Var, Lam, App, Num, Prim, Let,
  toANF, isInANF, evalANF, resetAnf
} from './anf.js';

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
// Basic ANF conversion
// ============================================================

test('number: already atomic', () => {
  resetAnf();
  const anf = toANF(new Num(42));
  assert.equal(anf.tag, 'ANum');
  assert.equal(anf.n, 42);
});

test('variable: already atomic', () => {
  resetAnf();
  const anf = toANF(new Var('x'));
  assert.equal(anf.tag, 'AVar');
});

test('lambda: body converted', () => {
  resetAnf();
  const anf = toANF(new Lam('x', new Var('x')));
  assert.equal(anf.tag, 'ALam');
  assert.equal(anf.param, 'x');
});

test('simple application: both sides atomic', () => {
  resetAnf();
  const anf = toANF(new App(new Var('f'), new Num(5)));
  assert.equal(anf.tag, 'CApp');
});

// ============================================================
// Nested expressions get let-bound
// ============================================================

test('nested app: f(g(x)) → let t = g(x) in f(t)', () => {
  resetAnf();
  const expr = new App(new Var('f'), new App(new Var('g'), new Var('x')));
  const anf = toANF(expr);
  // Should have a let binding
  assert.equal(anf.tag, 'ALet');
  // The complex part should be g(x)
  assert.equal(anf.complex.tag, 'CApp');
});

test('nested prim: (a+b) * (c+d) → let t1 = a+b in let t2 = c+d in t1*t2', () => {
  resetAnf();
  const expr = new Prim('*', new Prim('+', new Var('a'), new Var('b')), new Prim('+', new Var('c'), new Var('d')));
  const anf = toANF(expr);
  assert.equal(anf.tag, 'ALet');
});

// ============================================================
// ANF validation
// ============================================================

test('converted expressions are in ANF', () => {
  resetAnf();
  const exprs = [
    new Num(42),
    new Var('x'),
    new App(new Var('f'), new App(new Var('g'), new Var('x'))),
    new Prim('+', new Prim('*', new Var('a'), new Var('b')), new Var('c')),
  ];
  for (const e of exprs) {
    const anf = toANF(e);
    assert.ok(isInANF(anf), `Not in ANF: ${anf}`);
  }
});

// ============================================================
// Evaluation preserves semantics
// ============================================================

test('eval: (λx.x) 42 → 42', () => {
  resetAnf();
  const anf = toANF(new App(new Lam('x', new Var('x')), new Num(42)));
  assert.equal(evalANF(anf), 42);
});

test('eval: (λx. x+1) 41 → 42', () => {
  resetAnf();
  const anf = toANF(new App(new Lam('x', new Prim('+', new Var('x'), new Num(1))), new Num(41)));
  assert.equal(evalANF(anf), 42);
});

test('eval: let x = 5 in x + 1 → 6', () => {
  resetAnf();
  const anf = toANF(new Let('x', new Num(5), new Prim('+', new Var('x'), new Num(1))));
  assert.equal(evalANF(anf), 6);
});

test('eval: nested: let x=2 in let y=3 in x*y → 6', () => {
  resetAnf();
  const anf = toANF(new Let('x', new Num(2), new Let('y', new Num(3), new Prim('*', new Var('x'), new Var('y')))));
  assert.equal(evalANF(anf), 6);
});

// ============================================================
// Report
// ============================================================

console.log(`\nANF conversion tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
