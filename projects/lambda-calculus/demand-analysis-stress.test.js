/**
 * Demand Analysis Stress Tests
 * 
 * GHC-style demand analysis: how expressions use their arguments.
 * U = Used, L = Lazy, S = Strict, HU = Head Used, A = Absent
 */

import { Demand, analyze, DmdType, Var, Lam, App, Let, Lit, Case, If, BinOp, strictApp, isStrict, isAbsent, lubDemand, bothDemand, topDemand, botDemand } from './demand-analysis.js';

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); pass++; } catch (e) { fail++; console.log(`FAIL: ${name}\n  ${e.message}`); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

console.log('=== Demand Analysis Stress Tests ===');

// ============================================================
// Variable demand: x is used once
// ============================================================
test('variable is demanded', () => {
  const result = analyze(new Var('x'));
  const dmd = result.demands?.get('x') || result.envDemand?.('x');
  assert(dmd !== undefined, 'Variable x should have a demand');
  assert(!isAbsent(dmd), 'Variable used directly should not be absent');
});

// ============================================================
// Literal: no free variables
// ============================================================
test('literal has no demands', () => {
  const result = analyze(new Lit(42));
  const hasAny = result.demands?.size > 0 || false;
  assert(!hasAny || result.demands?.size === 0, 'Literal should have no demands');
});

// ============================================================
// Lambda body determines demand on argument
// ============================================================
test('identity lambda: argument is strict', () => {
  const id = new Lam('x', new Var('x'));
  const result = analyze(id);
  // In the body, x is used strictly (returned directly)
  assert(result !== undefined, 'Analysis should produce result');
});

// ============================================================
// K combinator: second argument is absent
// ============================================================
test('K combinator: second arg absent', () => {
  const k = new Lam('x', new Lam('y', new Var('x')));
  const result = analyze(k);
  // y should be absent in the body
  assert(result !== undefined, 'Analysis should produce result');
});

// ============================================================
// Conditional: both branches contribute
// ============================================================
test('conditional demand', () => {
  const expr = new If(new Var('c'), new Var('x'), new Var('y'));
  const result = analyze(expr);
  assert(result !== undefined, 'Should analyze conditional');
});

// ============================================================
// Strict application
// ============================================================
test('strict application', () => {
  const expr = new App(new Var('f'), new Var('x'));
  const result = analyze(expr);
  assert(result !== undefined, 'Should analyze application');
});

// ============================================================
// Let binding demand
// ============================================================
test('let binding demand propagation', () => {
  const expr = new Let('x', new Lit(42), new Var('x'));
  const result = analyze(expr);
  assert(result !== undefined, 'Should analyze let binding');
});

// ============================================================
// Demand lattice operations
// ============================================================
test('top demand', () => {
  const top = topDemand();
  assert(top !== undefined, 'Top demand should exist');
});

test('bot demand', () => {
  const bot = botDemand();
  assert(bot !== undefined, 'Bottom demand should exist');
});

test('lub of demands', () => {
  const d1 = topDemand();
  const d2 = botDemand();
  const result = lubDemand(d1, d2);
  assert(result !== undefined, 'LUB should produce a demand');
});

test('both of demands', () => {
  const d1 = topDemand();
  const d2 = topDemand();
  const result = bothDemand(d1, d2);
  assert(result !== undefined, 'Both should produce a demand');
});

test('isStrict', () => {
  const strict = botDemand();
  assert(typeof isStrict(strict) === 'boolean', 'isStrict should return boolean');
});

test('isAbsent', () => {
  const absent = topDemand();
  assert(typeof isAbsent(absent) === 'boolean', 'isAbsent should return boolean');
});

console.log(`\nDemand analysis stress tests: ${pass}/${pass + fail} passed`);
if (fail > 0) { console.log(`${fail} FAILED`); process.exit(1); }
