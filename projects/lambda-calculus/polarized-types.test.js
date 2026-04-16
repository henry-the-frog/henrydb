import { strict as assert } from 'assert';
import { POS, NEG, TSum, TFun, TWith, polarity, VInj, VPair, VUnit, VThunk, CLam, CWith, CReturn, isFocused, isNeutral, apply, projectFst, projectSnd, matchSum, force } from './polarized-types.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('polarity: sum is positive', () => assert.equal(new TSum('Bool', ['True', 'False']).polarity, POS));
test('polarity: function is negative', () => assert.equal(new TFun(null, null).polarity, NEG));
test('polarity: with is negative', () => assert.equal(new TWith(null, null).polarity, NEG));

test('value: injection is focused', () => assert.ok(isFocused(new VInj('Left', 42))));
test('value: pair is focused', () => assert.ok(isFocused(new VPair(1, 2))));
test('comp: lambda is neutral', () => assert.ok(isNeutral(new CLam(x => x))));
test('comp: with is neutral', () => assert.ok(isNeutral(new CWith(1, 2))));

test('apply: lambda to argument', () => {
  const fn = new CLam(x => new CReturn(x * 2));
  const result = apply(fn, 21);
  assert.equal(result.val, 42);
});

test('project: fst of with-pair', () => {
  const pair = new CWith('hello', 'world');
  assert.equal(projectFst(pair), 'hello');
});

test('matchSum: Left', () => {
  const val = new VInj('Left', 42);
  const result = matchSum(val, { Left: x => x * 2, Right: x => x });
  assert.equal(result, 84);
});

test('thunk/force: freeze and unfreeze', () => {
  const comp = new CLam(x => x);
  const thunked = new VThunk(comp);
  assert.ok(isFocused(thunked));
  assert.equal(force(thunked), comp);
});

console.log(`\nPolarized types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
