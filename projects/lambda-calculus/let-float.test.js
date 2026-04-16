import { strict as assert } from 'assert';
import { Var, Lam, App, Let, Num, floatOut, floatIn, letDepth } from './let-float.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// Float out
test('floatOut: let from inside lambda', () => {
  // λx. let y = 5 in x + y  →  let y = 5 in λx. x + y
  const expr = new Lam('x', new Let('y', new Num(5), new App(new Var('x'), new Var('y'))));
  const r = floatOut(expr);
  assert.equal(r.tag, 'Let'); // Let is now outside
  assert.equal(r.body.tag, 'Lam');
});

test('floatOut: dont float if init uses lambda var', () => {
  // λx. let y = x in y  →  stays (y depends on x)
  const expr = new Lam('x', new Let('y', new Var('x'), new Var('y')));
  const r = floatOut(expr);
  assert.equal(r.tag, 'Lam');
  assert.equal(r.body.tag, 'Let');
});

test('floatOut: preserves when no lets', () => {
  const expr = new Lam('x', new Var('x'));
  assert.equal(floatOut(expr).tag, 'Lam');
});

// Float in
test('floatIn: let into fn of app', () => {
  // let x = 5 in (x y)  →  (let x = 5 in x) y  (if only in fn)
  const expr = new Let('x', new Num(5), new App(new Var('x'), new Var('y')));
  const r = floatIn(expr);
  assert.equal(r.tag, 'App');
  assert.equal(r.fn.tag, 'Let');
});

test('floatIn: let into arg of app', () => {
  // let x = 5 in (f x)  →  f (let x = 5 in x)
  const expr = new Let('x', new Num(5), new App(new Var('f'), new Var('x')));
  const r = floatIn(expr);
  assert.equal(r.tag, 'App');
  assert.equal(r.arg.tag, 'Let');
});

test('floatIn: used in both → stays', () => {
  const expr = new Let('x', new Num(5), new App(new Var('x'), new Var('x')));
  const r = floatIn(expr);
  assert.equal(r.tag, 'Let'); // Can't float into just one side
});

// Let depth
test('letDepth: top-level let = depth 0', () => {
  const expr = new Let('x', new Num(5), new Var('x'));
  assert.equal(letDepth(expr)[0].depth, 0);
});

test('letDepth: let inside lambda = depth 1', () => {
  const expr = new Lam('f', new Let('x', new Num(5), new Var('x')));
  assert.equal(letDepth(expr)[0].depth, 1);
});

test('floatOut reduces depth', () => {
  const expr = new Lam('f', new Let('x', new Num(5), new App(new Var('f'), new Var('x'))));
  const before = letDepth(expr)[0].depth;
  const after = letDepth(floatOut(expr))[0].depth;
  assert.ok(after < before);
});

test('simple: no transform needed', () => {
  const expr = new Var('x');
  assert.equal(floatOut(expr).tag, 'Var');
  assert.equal(floatIn(expr).tag, 'Var');
});

console.log(`\nLet floating tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
