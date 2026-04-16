/**
 * Game Semantics Stress Tests
 */

import { Arena, Move, Strategy, identityStrategy, composeStrategies, applicationArena, lambdaArena, productArena, computeDenotation, isInnocent, isWellBracketed } from './game-semantics.js';

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); pass++; } catch (e) { fail++; console.log(`FAIL: ${name}\n  ${e.message}`); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

console.log('=== Game Semantics Stress Tests ===');

test('identity strategy exists', () => {
  const id = identityStrategy();
  assert(id !== undefined, 'Identity strategy should exist');
  assert(id.play !== undefined || id.moves !== undefined, 'Strategy should have play or moves');
});

test('arena creation', () => {
  const a = new Arena(['q'], ['a']); // One question, one answer
  assert(a.questions.length === 1 || a.moves, 'Arena should have structure');
});

test('function arena', () => {
  const base = new Arena(['q'], ['a']);
  const fn = lambdaArena(base, base);
  assert(fn !== undefined, 'Lambda arena should exist');
});

test('product arena', () => {
  const a1 = new Arena(['q1'], ['a1']);
  const a2 = new Arena(['q2'], ['a2']);
  const prod = productArena(a1, a2);
  assert(prod !== undefined, 'Product arena should exist');
});

test('application arena', () => {
  const base = new Arena(['q'], ['a']);
  const fn = lambdaArena(base, base);
  const app = applicationArena(fn, base);
  assert(app !== undefined, 'Application arena should exist');
});

test('identity strategy is innocent', () => {
  const id = identityStrategy();
  assert(isInnocent(id) === true, 'Identity should be innocent');
});

test('identity strategy is well-bracketed', () => {
  const id = identityStrategy();
  assert(isWellBracketed(id) === true, 'Identity should be well-bracketed');
});

test('strategy composition', () => {
  const s1 = identityStrategy();
  const s2 = identityStrategy();
  const composed = composeStrategies(s1, s2);
  assert(composed !== undefined, 'Composed strategy should exist');
});

test('denotation of identity', () => {
  const base = new Arena(['q'], ['a']);
  const term = { tag: 'Lam', param: 'x', body: { tag: 'Var', name: 'x' } };
  const den = computeDenotation(term, base);
  assert(den !== undefined, 'Denotation should exist');
});

test('composition preserves innocence', () => {
  const s1 = identityStrategy();
  const s2 = identityStrategy();
  const composed = composeStrategies(s1, s2);
  const innocent = isInnocent(composed);
  assert(innocent === true, 'Composition of innocent strategies should be innocent');
});

console.log(`\nGame semantics stress tests: ${pass}/${pass + fail} passed`);
if (fail > 0) { console.log(`${fail} FAILED`); process.exit(1); }
