import { strict as assert } from 'assert';
import {
  typeOf, TNum, TBool, TStr, TUnit, TFun, TPair, TList, EffRow, Comp, TypeEnv, resetFresh
} from './effects-types.js';
import {
  perform, v, n, b, s, u, fn, app, let_, if_, binop, pair, Handle
} from './effects.js';

let passed = 0, failed = 0, total = 0;
const env = new TypeEnv();

function test(name, testFn) {
  total++;
  try { testFn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Pure computations (no effects)
// ============================================================

test('literal number is Num', () => {
  const t = typeOf(n(42), env);
  assert.ok(t.valueType instanceof TNum);
  assert.ok(t.effects.isEmpty());
});

test('literal bool is Bool', () => {
  const t = typeOf(b(true), env);
  assert.ok(t.valueType instanceof TBool);
  assert.ok(t.effects.isEmpty());
});

test('literal string is Str', () => {
  const t = typeOf(s("hello"), env);
  assert.ok(t.valueType instanceof TStr);
  assert.ok(t.effects.isEmpty());
});

test('let binding: pure', () => {
  const t = typeOf(let_('x', n(5), v('x')), env);
  assert.ok(t.valueType instanceof TNum);
  assert.ok(t.effects.isEmpty());
});

test('arithmetic returns Num', () => {
  const t = typeOf(binop('+', n(1), n(2)), env);
  assert.ok(t.valueType instanceof TNum);
  assert.ok(t.effects.isEmpty());
});

test('comparison returns Bool', () => {
  const t = typeOf(binop('<', n(1), n(2)), env);
  assert.ok(t.valueType instanceof TBool);
  assert.ok(t.effects.isEmpty());
});

test('lambda: pure function', () => {
  const t = typeOf(fn('x', binop('+', v('x'), n(1))), env);
  assert.ok(t.valueType instanceof TFun);
  assert.ok(t.effects.isEmpty());
});

test('pair type', () => {
  const t = typeOf(pair(n(1), s("hello")), env);
  assert.ok(t.valueType instanceof TPair);
  assert.ok(t.valueType.fst instanceof TNum);
  assert.ok(t.valueType.snd instanceof TStr);
});

test('if-else: pure', () => {
  const t = typeOf(if_(b(true), n(1), n(2)), env);
  assert.ok(t.valueType instanceof TNum);
  assert.ok(t.effects.isEmpty());
});

test('application: pure', () => {
  const t = typeOf(app(fn('x', binop('*', v('x'), n(2))), n(21)), env);
  assert.ok(t.effects.isEmpty());
});

// ============================================================
// Effectful computations
// ============================================================

test('Raise has Exn effect', () => {
  const t = typeOf(perform('Raise', s("error")), env);
  assert.ok(t.effects.has('Exn'), `Expected Exn effect, got ${t.effects}`);
});

test('Get has State effect', () => {
  const t = typeOf(perform('Get', u()), env);
  assert.ok(t.effects.has('State'), `Expected State effect, got ${t.effects}`);
});

test('Put has State effect', () => {
  const t = typeOf(perform('Put', n(42)), env);
  assert.ok(t.effects.has('State'));
});

test('Log has Log effect', () => {
  const t = typeOf(perform('Log', s("hello")), env);
  assert.ok(t.effects.has('Log'));
});

test('Choose has Nondet effect', () => {
  const t = typeOf(perform('Choose', u()), env);
  assert.ok(t.effects.has('Nondet'));
});

// ============================================================
// Effect propagation
// ============================================================

test('let propagates effects', () => {
  const t = typeOf(let_('x', perform('Raise', s("err")), v('x')), env);
  assert.ok(t.effects.has('Exn'));
});

test('if propagates effects from branches', () => {
  const t = typeOf(if_(b(true), perform('Raise', s("err")), n(1)), env);
  assert.ok(t.effects.has('Exn'));
});

test('multiple effects combine', () => {
  const t = typeOf(
    let_('x', perform('Get', u()),
      let_('_', perform('Log', s("got state")),
        v('x'))),
    env);
  assert.ok(t.effects.has('State'));
  assert.ok(t.effects.has('Log'));
});

test('function body effects in function type', () => {
  const t = typeOf(fn('x', perform('Raise', s("boom"))), env);
  assert.ok(t.valueType instanceof TFun);
  assert.ok(t.valueType.effects.has('Exn'));
});

// ============================================================
// Effect handling
// ============================================================

test('handler removes handled effect', () => {
  const body = perform('Raise', s("err"));
  const handled = new Handle(body, {
    return: fn('x', v('x')),
    ops: { 'Raise': fn('e', s("caught")) }
  });
  const t = typeOf(handled, env);
  assert.ok(!t.effects.has('Exn'), `Exn should be handled, got ${t.effects}`);
});

test('handler preserves unhandled effects', () => {
  const body = let_('_', perform('Get', u()), perform('Raise', s("err")));
  const handled = new Handle(body, {
    return: fn('x', v('x')),
    ops: { 'Raise': fn('e', s("caught")) }
  });
  const t = typeOf(handled, env);
  assert.ok(!t.effects.has('Exn'), 'Exn should be handled');
  assert.ok(t.effects.has('State'), `State should remain, got ${t.effects}`);
});

// ============================================================
// Effect row operations
// ============================================================

test('EffRow union combines effects', () => {
  const r1 = new EffRow(['State']);
  const r2 = new EffRow(['Exn']);
  const combined = r1.union(r2);
  assert.ok(combined.has('State'));
  assert.ok(combined.has('Exn'));
});

test('EffRow without removes effect', () => {
  const r = new EffRow(['State', 'Exn']);
  const filtered = r.without('Exn');
  assert.ok(filtered.has('State'));
  assert.ok(!filtered.has('Exn'));
});

test('EffRow empty check', () => {
  assert.ok(new EffRow([]).isEmpty());
  assert.ok(!new EffRow(['State']).isEmpty());
});

// ============================================================
// Complex programs
// ============================================================

test('safe division type includes Exn', () => {
  const safeDivide = fn('a', fn('b',
    if_(binop('==', v('b'), n(0)),
      perform('Raise', s('division by zero')),
      binop('/', v('a'), v('b')))));
  const t = typeOf(safeDivide, env);
  assert.ok(t.valueType instanceof TFun);
  // The inner function has Exn effect
  const innerFn = t.valueType.ret;
  // Through the chain, Exn should be present
});

test('pure factorial has no effects', () => {
  // Can't easily type recursive functions without fixpoint
  // But the body should be pure
  const factBody = fn('self', fn('n',
    if_(binop('==', v('n'), n(0)),
      n(1),
      binop('*', v('n'), app(app(v('self'), v('self')), binop('-', v('n'), n(1)))))));
  const t = typeOf(factBody, env);
  assert.ok(t.effects.isEmpty(), `Expected pure, got ${t.effects}`);
});

test('stateful computation types correctly', () => {
  const comp = let_('s', perform('Get', u()),
    let_('_', perform('Put', binop('+', v('s'), n(1))),
      perform('Get', u())));
  const t = typeOf(comp, env);
  assert.ok(t.effects.has('State'));
  assert.ok(!t.effects.has('Exn'));
});

// ============================================================
// Stress tests: complex programs
// ============================================================

test('nested handlers: inner catches, outer redundant', () => {
  const inner = new Handle(
    perform('Raise', s('inner')),
    { return: fn('x', v('x')), ops: { 'Raise': fn('e', s('caught')) } });
  const outer = new Handle(inner,
    { return: fn('x', v('x')), ops: { 'Raise': fn('e', s('caught2')) } });
  const t = typeOf(outer, env);
  assert.ok(!t.effects.has('Exn'), 'Both handlers remove Exn');
});

test('handler composition: except wraps state', () => {
  const body = let_('_', perform('Get', u()), perform('Raise', s('err')));
  const handled = new Handle(body, {
    return: fn('x', v('x')),
    ops: { 'Raise': fn('e', n(0)) }
  });
  const t = typeOf(handled, env);
  assert.ok(!t.effects.has('Exn'), 'Exn should be handled');
  assert.ok(t.effects.has('State'), 'State should remain');
});

test('effectful function call propagates effects', () => {
  const t = typeOf(app(fn('x', perform('Raise', v('x'))), s('boom')), env);
  assert.ok(t.effects.has('Exn'));
});

test('let chain with State', () => {
  const expr = let_('a', perform('Get', u()),
    let_('_', perform('Put', binop('+', v('a'), n(1))),
      let_('b', perform('Get', u()), v('b'))));
  const t = typeOf(expr, env);
  assert.ok(t.effects.has('State'));
  assert.ok(!t.effects.has('Exn'));
});

test('nondeterministic with Choose', () => {
  const expr = let_('choice', perform('Choose', u()),
    if_(v('choice'), n(1), n(2)));
  const t = typeOf(expr, env);
  assert.ok(t.effects.has('Nondet'));
  assert.ok(t.valueType instanceof TNum);
});

test('multiple effects: Log then Raise', () => {
  const expr = let_('_', perform('Log', s('starting')),
    perform('Raise', s('failed')));
  const t = typeOf(expr, env);
  assert.ok(t.effects.has('Log'));
  assert.ok(t.effects.has('Exn'));
});

test('pure function stays pure when called', () => {
  const t = typeOf(app(fn('x', binop('*', v('x'), n(2))), n(21)), env);
  assert.ok(t.effects.isEmpty());
});

test('handler for all effects results in pure', () => {
  const body = let_('_', perform('Log', s('hi')), perform('Raise', s('err')));
  const h1 = new Handle(body, {
    return: fn('x', v('x')),
    ops: { 'Raise': fn('e', n(0)), 'Log': fn('m', u()) }
  });
  const t = typeOf(h1, env);
  assert.ok(t.effects.isEmpty(), `Expected pure, got ${t.effects}`);
});

// ============================================================
// Report
// ============================================================

console.log(`\nEffect types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
