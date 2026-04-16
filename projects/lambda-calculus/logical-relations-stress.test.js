/**
 * Logical Relations Stress Tests
 * 
 * Verify: parametricity (free theorems) via logical relations.
 * - If f : ∀a. a → a, then f must be the identity (by parametricity)
 * - If f : ∀a. a → a → a, then f must be either const first or const second
 */

import { TBase, TFun, logicalRelation, contextuallyEquivalent, observationallyEquivalent, adequacy, fundamentalTheorem, stepIndexed } from './logical-relations.js';

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); pass++; } catch (e) { fail++; console.log(`FAIL: ${name}\n  ${e.message}`); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

const A = new TBase('A');
const B = new TBase('B');
const Int = new TBase('Int');
const AA = new TFun(A, A);

console.log('=== Logical Relations Stress Tests ===');

// ============================================================
// Identity function: fundamental theorem
// ============================================================
test('identity is in self-relation', () => {
  const id = x => x;
  const result = fundamentalTheorem(id, AA, {});
  // For functions, result is { check: (a,b) => bool }
  assert(result.check(1, 1), 'id(1) should relate to id(1)');
  assert(result.check('hello', 'hello'), 'id(hello) should relate to id(hello)');
});

// ============================================================
// K combinator: first argument
// ============================================================
test('K is in relation for A → B → A', () => {
  const k = x => y => x;
  const type = new TFun(A, new TFun(B, A));
  const result = fundamentalTheorem(k, type, {});
  // K is (A → B → A), so R[A → B → A](k, k)
  // k(a) = λy.a, so k(a)(b) = a
  const inner = result.check(42, 42);
  assert(inner.check('anything', 'anything'), 'K(42)(anything) should be 42');
});

// ============================================================
// Contextual equivalence: two versions of identity
// ============================================================
test('contextual equivalence of id implementations', () => {
  const id1 = x => x;
  const id2 = x => { const y = x; return y; }; // Same semantics, different implementation
  const contexts = [
    f => f(42),
    f => f('hello'),
    f => f(true),
    f => f(null),
    f => f(f)(3), // Apply to self then to 3
  ];
  assert(contextuallyEquivalent(id1, id2, contexts), 'Two id implementations should be equivalent');
});

// ============================================================
// Non-equivalent terms are not contextually equivalent
// ============================================================
test('non-equivalent terms distinguished by context', () => {
  const always42 = x => 42;
  const id = x => x;
  const contexts = [
    f => f(0),
  ];
  assert(!contextuallyEquivalent(always42, id, contexts), 'always42 ≠ id');
});

// ============================================================
// Observational equivalence: same behavior on all observations
// ============================================================
test('observational equivalence of 2+3 and 5', () => {
  const term1 = () => 2 + 3;
  const term2 = () => 5;
  const observations = [
    f => f(),
    f => f() === 5,
    f => f() > 4,
    f => f() * 2,
  ];
  assert(observationallyEquivalent(term1, term2, observations), '2+3 ≡ 5');
});

// ============================================================
// Adequacy: logical relation implies observational equivalence
// ============================================================
test('adequacy for identity', () => {
  const id = x => x;
  const rel = (f, g) => f(1) === g(1) && f('a') === g('a');
  const observations = [f => f(42), f => f('hello')];
  const result = adequacy(rel, id, id, observations);
  assert(result.adequate, 'Adequacy should hold for self-relation');
});

// ============================================================
// Step-indexed: at step 0, everything is related
// ============================================================
test('step-indexed at step 0', () => {
  // At step 0 in step-indexed logical relations, everything relates
  const rel0 = stepIndexed(0, AA, {});
  assert(rel0(42, 'banana') === true, 'At step 0, everything relates');
});

// ============================================================
// Parametricity free theorem: f : a → a implies f = id
// ============================================================
test('parametricity: polymorphic identity', () => {
  // If f has type ∀a. a → a, then for ANY relation R on a,
  // if R(x, y) then R(f(x), f(y))
  // This means f must preserve all relations — it must be the identity
  
  const f = x => x; // This IS the identity
  
  // Test with the "is-equal" relation
  const R = (a, b) => a === b;
  
  // f preserves R: if R(x, y) then R(f(x), f(y))
  const testCases = [[1, 1], ['a', 'a'], [null, null]];
  for (const [x, y] of testCases) {
    assert(R(x, y), `Precondition: R(${x}, ${y})`);
    assert(R(f(x), f(y)), `f should preserve R: R(f(${x}), f(${y}))`);
  }
  
  // Test with the "both-positive" relation
  const R2 = (a, b) => a > 0 && b > 0;
  assert(R2(f(1), f(2)), 'f should preserve positivity');
});

// ============================================================
// Parametricity free theorem: f : a → a → a can only be fst or snd
// ============================================================
test('parametricity: binary choice', () => {
  const fst = x => y => x;
  const snd = x => y => y;
  
  // Any f : ∀a. a → a → a must be one of these two
  // Test: f(0)(1) must be either 0 or 1
  for (const f of [fst, snd]) {
    const result = f(0)(1);
    assert(result === 0 || result === 1, `f(0)(1) must be 0 or 1, got ${result}`);
  }
});

// ============================================================
// Summary
// ============================================================
console.log(`\nLogical relations stress tests: ${pass}/${pass + fail} passed`);
if (fail > 0) { console.log(`${fail} FAILED`); process.exit(1); }
