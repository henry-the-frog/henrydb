import { strict as assert } from 'assert';
import { Pos, Neg, Atom, polarity, invert, focus, search } from './focusing.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const A = new Atom('A'), B = new Atom('B');

test('polarity: atom → neutral', () => assert.equal(polarity(A), 'neutral'));
test('polarity: ∧ → positive', () => assert.equal(polarity(new Pos('∧', [A, B])), 'positive'));
test('polarity: → → negative', () => assert.equal(polarity(new Neg('→', [A, B])), 'negative'));

test('invert: A→B → subgoals', () => {
  const r = invert(new Neg('→', [A, B]));
  assert.ok(r.invertible);
  assert.equal(r.subgoals.length, 2);
});

test('invert: A∧B → subgoals', () => {
  const r = invert(new Pos('∧', [A, B]));
  assert.ok(r.invertible);
});

test('invert: atom → not invertible', () => {
  assert.ok(!invert(A).invertible);
});

test('focus: A∨B → 2 choices', () => {
  const r = focus(new Pos('∨', [A, B]));
  assert.equal(r.choices.length, 2);
});

test('search: axiom (A in hyps)', () => {
  const r = search(A, [A]);
  assert.ok(r);
  assert.equal(r.proof, 'axiom');
});

test('search: A∧B from A, B', () => {
  const r = search(new Pos('∧', [A, B]), [A, B]);
  assert.ok(r);
  assert.equal(r.proof, 'and-right');
});

test('search: impossible → null', () => {
  assert.equal(search(B, [A], 3), null);
});

console.log(`\nFocusing tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
