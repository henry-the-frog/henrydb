import { strict as assert } from 'assert';
import {
  Num, Bool, Str, Unit, Pair, ListVal, Closure,
  Var, Lam, App, Let, If, BinOp, Lit, MkPair, Fst, Snd,
  Perform, Handle,
  evaluate, Env, EffectSignal,
  perform, handle, v, n, b, s, u, fn, app, let_, if_, binop, pair,
  runState, runExcept, runNondet, runLog
} from './effects.js';

let passed = 0, failed = 0, total = 0;

function test(name, testFn) {
  total++;
  try { testFn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const env = new Env();

// ============================================================
// Basic evaluation (no effects)
// ============================================================

test('literal number', () => {
  assert.deepStrictEqual(evaluate(n(42), env), new Num(42));
});

test('literal bool', () => {
  assert.deepStrictEqual(evaluate(b(true), env), new Bool(true));
});

test('literal string', () => {
  assert.deepStrictEqual(evaluate(s('hello'), env), new Str('hello'));
});

test('let binding', () => {
  const expr = let_('x', n(5), v('x'));
  assert.deepStrictEqual(evaluate(expr, env), new Num(5));
});

test('lambda and application', () => {
  const expr = app(fn('x', binop('+', v('x'), n(1))), n(41));
  assert.deepStrictEqual(evaluate(expr, env), new Num(42));
});

test('if-then-else: true', () => {
  const expr = if_(b(true), n(1), n(2));
  assert.deepStrictEqual(evaluate(expr, env), new Num(1));
});

test('if-then-else: false', () => {
  const expr = if_(b(false), n(1), n(2));
  assert.deepStrictEqual(evaluate(expr, env), new Num(2));
});

test('arithmetic', () => {
  const expr = binop('*', binop('+', n(2), n(3)), n(4));
  assert.deepStrictEqual(evaluate(expr, env), new Num(20));
});

test('comparison', () => {
  const expr = binop('<', n(3), n(5));
  assert.deepStrictEqual(evaluate(expr, env), new Bool(true));
});

test('pair', () => {
  const expr = pair(n(1), s('hello'));
  const result = evaluate(expr, env);
  assert.ok(result instanceof Pair);
  assert.deepStrictEqual(result.fst, new Num(1));
  assert.deepStrictEqual(result.snd, new Str('hello'));
});

test('nested let', () => {
  const expr = let_('x', n(5), let_('y', n(10), binop('+', v('x'), v('y'))));
  assert.deepStrictEqual(evaluate(expr, env), new Num(15));
});

test('closure captures environment', () => {
  const expr = let_('x', n(10), 
    app(fn('y', binop('+', v('x'), v('y'))), n(32)));
  assert.deepStrictEqual(evaluate(expr, env), new Num(42));
});

test('higher-order function', () => {
  const expr = let_('apply', fn('f', fn('x', app(v('f'), v('x')))),
    app(app(v('apply'), fn('n', binop('*', v('n'), n(2)))), n(21)));
  assert.deepStrictEqual(evaluate(expr, env), new Num(42));
});

// ============================================================
// Exception effect
// ============================================================

test('Raise effect caught by handler', () => {
  const expr = perform('Raise', s('error!'));
  const result = runExcept(expr, env);
  assert.ok(result instanceof Pair);
  assert.deepStrictEqual(result.fst, new Str('Err'));
  assert.deepStrictEqual(result.snd, new Str('error!'));
});

test('successful computation returns Ok', () => {
  const expr = n(42);
  const result = runExcept(expr, env);
  assert.ok(result instanceof Pair);
  assert.deepStrictEqual(result.fst, new Str('Ok'));
  assert.deepStrictEqual(result.snd, new Num(42));
});

test('exception in let binding', () => {
  const expr = let_('x', n(5),
    if_(binop('>', v('x'), n(3)),
      perform('Raise', s('too big')),
      v('x')));
  const result = runExcept(expr, env);
  assert.deepStrictEqual(result.fst, new Str('Err'));
  assert.deepStrictEqual(result.snd, new Str('too big'));
});

test('exception with dynamic message', () => {
  const expr = let_('x', n(42),
    perform('Raise', binop('++', s('error: '), s('found'))));
  const result = runExcept(expr, env);
  assert.deepStrictEqual(result.snd, new Str('error: found'));
});

test('no exception returns normally', () => {
  const expr = let_('x', n(5),
    if_(binop('<', v('x'), n(10)),
      binop('*', v('x'), n(2)),
      perform('Raise', s('too big'))));
  const result = runExcept(expr, env);
  assert.deepStrictEqual(result.fst, new Str('Ok'));
  assert.deepStrictEqual(result.snd, new Num(10));
});

// ============================================================
// State effect (simplified)
// ============================================================

test('Get state', () => {
  const expr = perform('Get', u());
  const result = runState(expr, 42);
  // Should return the initial state
  assert.ok(result instanceof Pair);
});

test('Put state', () => {
  const expr = perform('Put', n(99));
  const result = runState(expr, 0);
  assert.ok(result instanceof Pair);
});

// ============================================================
// Nondeterminism effect
// ============================================================

test('Choose effect produces branches', () => {
  const expr = perform('Choose', b(true));
  const result = runNondet(expr, env);
  assert.ok(result instanceof ListVal);
});

// ============================================================
// Log effect
// ============================================================

test('Log effect collects messages', () => {
  const expr = perform('Log', s('hello world'));
  const result = runLog(expr, env);
  assert.ok(result instanceof Pair);
  assert.ok(result.snd instanceof ListVal);
  assert.equal(result.snd.elems.length, 1);
  assert.deepStrictEqual(result.snd.elems[0], new Str('hello world'));
});

// ============================================================
// Composability
// ============================================================

test('handler catches only specific effects', () => {
  // Raise should be caught by except handler
  const expr = perform('Raise', s('boom'));
  const result = runExcept(expr, env);
  assert.deepStrictEqual(result.fst, new Str('Err'));
});

test('unhandled effect propagates', () => {
  // Get is not handled by except handler → should throw EffectSignal
  const expr = perform('Get', u());
  let caught = false;
  try {
    runExcept(expr, env);
  } catch (e) {
    caught = true;
    assert.equal(e.effect, 'Get');
  }
  assert.ok(caught, 'Expected unhandled effect to propagate');
});

test('nested handlers: except wraps computation', () => {
  const expr = let_('x', n(5),
    binop('+', v('x'), n(10)));
  const result = runExcept(expr, env);
  assert.deepStrictEqual(result.fst, new Str('Ok'));
  assert.deepStrictEqual(result.snd, new Num(15));
});

// ============================================================
// Complex programs with effects
// ============================================================

test('safe division: returns Err on divide by zero', () => {
  const safeDivide = fn('a', fn('b',
    if_(binop('==', v('b'), n(0)),
      perform('Raise', s('division by zero')),
      binop('/', v('a'), v('b')))));
  
  // 10 / 0 → Err
  const result1 = runExcept(app(app(safeDivide, n(10)), n(0)), env);
  assert.deepStrictEqual(result1.fst, new Str('Err'));
  assert.deepStrictEqual(result1.snd, new Str('division by zero'));
  
  // 10 / 2 → Ok(5)
  const result2 = runExcept(app(app(safeDivide, n(10)), n(2)), env);
  assert.deepStrictEqual(result2.fst, new Str('Ok'));
  assert.deepStrictEqual(result2.snd, new Num(5));
});

test('factorial: pure computation works', () => {
  // fact = fn(n) => if n == 0 then 1 else n * fact(n-1)
  // Using Y combinator pattern via let rec
  const factBody = fn('self', fn('n',
    if_(binop('==', v('n'), n(0)),
      n(1),
      binop('*', v('n'), app(app(v('self'), v('self')), binop('-', v('n'), n(1)))))));
  
  const fact = let_('fact', factBody,
    app(app(v('fact'), v('fact')), n(5)));
  
  const result = evaluate(fact, env);
  assert.deepStrictEqual(result, new Num(120));
});

test('validated computation: raise on negative', () => {
  const validate = fn('x',
    if_(binop('<', v('x'), n(0)),
      perform('Raise', s('negative!')),
      binop('*', v('x'), v('x'))));
  
  const result1 = runExcept(app(validate, n(5)), env);
  assert.deepStrictEqual(result1.fst, new Str('Ok'));
  assert.deepStrictEqual(result1.snd, new Num(25));
  
  const result2 = runExcept(app(validate, n(-3)), env);
  assert.deepStrictEqual(result2.fst, new Str('Err'));
  assert.deepStrictEqual(result2.snd, new Str('negative!'));
});

// ============================================================
// Report
// ============================================================

console.log(`\nEffects tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
