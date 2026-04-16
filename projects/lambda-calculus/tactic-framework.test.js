import { strict as assert } from 'assert';
import { Goal, ProofState, intro, exact, assumption, split, left, right, then, orelse, tryTactic, prove } from './tactic-framework.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('intro: A → A', () => {
  const r = prove('A → A', [intro('h')]);
  assert.equal(r.current.target, 'A');
  assert.ok(r.current.hyps.has('h'));
});

test('exact: solve goal', () => {
  const r = prove('A → A', [intro('h'), exact('h')]);
  assert.ok(r.done);
});

test('assumption: auto-find', () => {
  const r = prove('A → A', [intro('h'), assumption]);
  assert.ok(r.done);
});

test('split: conjunction', () => {
  const r = prove('A → A ∧ A', [intro('h'), split]);
  assert.equal(r.goals.length, 2);
  assert.equal(r.goals[0].target, 'A');
  assert.equal(r.goals[1].target, 'A');
});

test('split + exact: prove A → A ∧ A', () => {
  const r = prove('A → A ∧ A', [intro('h'), split, exact('h'), exact('h')]);
  assert.ok(r.done);
});

test('left: disjunction', () => {
  const r = prove('A → A ∨ B', [intro('h'), left]);
  assert.equal(r.current.target, 'A');
});

test('right: disjunction', () => {
  const r = prove('A → B ∨ A', [intro('h'), right]);
  assert.equal(r.current.target, 'A');
});

test('then: compose tactics', () => {
  const tac = then(intro('h'), exact('h'));
  const r = tac(new ProofState([new Goal(new Map(), 'A → A')]));
  assert.ok(r.done);
});

test('orelse: fallback', () => {
  const tac = orelse(state => { throw new Error('fail'); }, assumption);
  const state = new ProofState([new Goal(new Map([['h', 'A']]), 'A')]);
  assert.ok(tac(state).done);
});

test('tryTactic: no-op on failure', () => {
  const state = new ProofState([new Goal(new Map(), 'A')]);
  const r = tryTactic(split)(state);
  assert.equal(r.goals.length, 1); // Unchanged
});

test('prove A → B → A', () => {
  const r = prove('A → B → A', [intro('a'), intro('b'), exact('a')]);
  assert.ok(r.done);
});

test('prove A → B → B', () => {
  const r = prove('A → B → B', [intro('a'), intro('b'), exact('b')]);
  assert.ok(r.done);
});

console.log(`\n🎉🎉🎉🎉🎉🎉🎉 MODULE #140! Tactic framework tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
