import { strict as assert } from 'assert';
import { play, trueStrategy, falseStrategy, constStrategy, compose, parallel, isWinning, interactionCount } from './game-semantics.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('true strategy: ? → true', () => {
  const h = play(null, trueStrategy, ['?']);
  assert.equal(h[1].label, 'true');
});

test('false strategy: ? → false', () => {
  const h = play(null, falseStrategy, ['?']);
  assert.equal(h[1].label, 'false');
});

test('const strategy: ? → 42', () => {
  const h = play(null, constStrategy(42), ['?']);
  assert.equal(h[1].label, '42');
});

test('play: O then P alternation', () => {
  const h = play(null, constStrategy(1), ['?']);
  assert.equal(h[0].player, 'O');
  assert.equal(h[1].player, 'P');
});

test('isWinning: true strategy wins on ?', () => {
  assert.ok(isWinning(trueStrategy, ['?']));
});

test('isWinning: true strategy loses on unknown', () => {
  assert.ok(!isWinning(trueStrategy, ['unknown']));
});

test('interactionCount: 1 Q, 1 A', () => {
  const h = play(null, trueStrategy, ['?']);
  const counts = interactionCount(h);
  assert.equal(counts.questions, 1);
  assert.equal(counts.answers, 1);
});

test('parallel: tries both strategies', () => {
  const combined = parallel(trueStrategy, falseStrategy);
  assert.equal(combined.respond('?', []), 'true');
});

test('compose: chained response', () => {
  const double = { name: 'double', respond: (m) => m === '5' ? '10' : null };
  const c = compose(double, constStrategy(5));
  assert.equal(c.respond('?', []), '10');
});

test('play: multiple rounds', () => {
  const echo = { name: 'echo', respond: (m) => `echo:${m}` };
  const h = play(null, echo, ['a', 'b', 'c']);
  assert.equal(h.length, 6); // 3 O + 3 P moves
});

console.log(`\nGame semantics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
