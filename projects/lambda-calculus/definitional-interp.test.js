import { strict as assert } from 'assert';
import { evalDirect, evalMonadic, Num, Bool, Var, Lam, App, Add, If, Let, Letrec, Print } from './definitional-interp.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('direct: num', () => assert.equal(evalDirect(Num(42)), 42));
test('direct: add', () => assert.equal(evalDirect(Add(Num(2), Num(3))), 5));
test('direct: lambda + app', () => assert.equal(evalDirect(App(Lam('x', Var('x')), Num(42))), 42));
test('direct: let', () => assert.equal(evalDirect(Let('x', Num(5), Add(Var('x'), Num(1)))), 6));
test('direct: if true', () => assert.equal(evalDirect(If(Bool(true), Num(1), Num(2))), 1));
test('direct: if false', () => assert.equal(evalDirect(If(Bool(false), Num(1), Num(2))), 2));
test('direct: letrec (factorial-like)', () => {
  const prog = Letrec('f', 'n', If(Var('n'), Add(Var('n'), App(Var('f'), Add(Var('n'), Num(-1)))), Num(0)),
    App(Var('f'), Num(3)));
  assert.equal(evalDirect(prog), 6); // 3+2+1+0
});
test('direct: K combinator', () => {
  const K = Lam('x', Lam('y', Var('x')));
  assert.equal(evalDirect(App(App(K, Num(1)), Num(2))), 1);
});
test('monadic: num', () => assert.equal(evalMonadic(Num(42)).value, 42));
test('monadic: add', () => assert.equal(evalMonadic(Add(Num(2), Num(3))).value, 5));
test('monadic: print tracks effect', () => {
  const r = evalMonadic(Print(Num(42)));
  assert.equal(r.effects.length, 1);
  assert.equal(r.effects[0].type, 'print');
  assert.equal(r.effects[0].value, 42);
});

console.log(`\n🎉🎉🎉 MODULE #170!!! Definitional interpreter tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
