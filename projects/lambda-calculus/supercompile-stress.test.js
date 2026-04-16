/**
 * Supercompiler Stress Tests
 */

import { Num, Var, Add, Mul, If0, drive, homeomorphicEmbedding, supercompile } from './supercompile.js';

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); pass++; } catch (e) { fail++; console.log(`FAIL: ${name}\n  ${e.message}`); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

console.log('=== Supercompiler Stress Tests ===');

// ============================================================
// Driving: one step of evaluation
// ============================================================
test('drive constant', () => {
  const result = drive(new Num(42));
  assert(result !== undefined, 'Drive should produce a result');
});

test('drive addition of constants', () => {
  const result = drive(new Add(new Num(3), new Num(4)));
  // Should constant-fold to 7
  assert(result.value === 7 || (result instanceof Num && result.n === 7),
    `3 + 4 should drive to 7`);
});

test('drive multiplication', () => {
  const result = drive(new Mul(new Num(6), new Num(7)));
  assert(result.value === 42 || (result instanceof Num && result.n === 42),
    `6 * 7 should drive to 42`);
});

test('drive if0 with 0', () => {
  const expr = new If0(new Num(0), new Num(1), new Num(2));
  const result = drive(expr);
  // If 0 then 1 else 2 → 1
  assert(result.value === 1 || (result instanceof Num && result.n === 1),
    `if0(0, 1, 2) should be 1`);
});

test('drive if0 with non-0', () => {
  const expr = new If0(new Num(5), new Num(1), new Num(2));
  const result = drive(expr);
  assert(result.value === 2 || (result instanceof Num && result.n === 2),
    `if0(5, 1, 2) should be 2`);
});

// ============================================================
// Homeomorphic embedding (for termination check)
// ============================================================
test('homeomorphic: Num embeds in Num', () => {
  const result = homeomorphicEmbedding(new Num(1), new Num(2));
  assert(result === true, 'Any Num should embed in any Num');
});

test('homeomorphic: Var embeds in Var', () => {
  const result = homeomorphicEmbedding(new Var('x'), new Var('y'));
  assert(typeof result === 'boolean', 'Should return boolean');
});

test('homeomorphic: smaller embeds in larger', () => {
  const small = new Num(1);
  const large = new Add(new Num(1), new Num(2));
  const result = homeomorphicEmbedding(small, large);
  assert(result === true, 'Num should embed in Add(Num, Num)');
});

test('homeomorphic: reflexive', () => {
  const expr = new Add(new Num(1), new Num(2));
  const result = homeomorphicEmbedding(expr, expr);
  assert(result === true, 'Same expression should embed in itself');
});

// ============================================================
// Supercompilation
// ============================================================
test('supercompile constant', () => {
  const result = supercompile(new Num(42));
  assert(result !== undefined, 'Should produce result');
  assert(result instanceof Num && result.n === 42, 'Constant should be preserved');
});

test('supercompile addition', () => {
  const result = supercompile(new Add(new Num(3), new Num(4)));
  assert(result instanceof Num && result.n === 7, `3+4 should supercompile to 7`);
});

test('supercompile nested arithmetic', () => {
  const expr = new Add(new Mul(new Num(2), new Num(3)), new Num(1));
  const result = supercompile(expr);
  assert(result instanceof Num && result.n === 7, `2*3+1 should supercompile to 7`);
});

test('supercompile if0', () => {
  const expr = new If0(new Num(0), new Add(new Num(1), new Num(2)), new Num(99));
  const result = supercompile(expr);
  assert(result instanceof Num && result.n === 3, `if0(0, 1+2, 99) should be 3`);
});

test('supercompile with variable', () => {
  const expr = new Add(new Var('x'), new Num(0));
  const result = supercompile(expr);
  // x + 0 could be optimized to just x
  assert(result !== undefined, 'Should handle variables');
});

// ============================================================
// Summary
// ============================================================
console.log(`\nSupercompiler stress tests: ${pass}/${pass + fail} passed`);
if (fail > 0) { console.log(`${fail} FAILED`); process.exit(1); }
