import { strict as assert } from 'assert';
import { SimpleArrow, KleisliArrow, loop, accumulator } from './arrows.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('arr: lift function', () => assert.equal(SimpleArrow.arr(x => x + 1).run(41), 42));

test('compose: sequential', () => {
  const inc = SimpleArrow.arr(x => x + 1);
  const dbl = SimpleArrow.arr(x => x * 2);
  assert.equal(inc.compose(dbl).run(5), 12); // (5+1)*2
});

test('first: run on first component', () => {
  const inc = SimpleArrow.arr(x => x + 1);
  assert.deepStrictEqual(inc.first().run([5, 'hello']), [6, 'hello']);
});

test('second: run on second component', () => {
  const dbl = SimpleArrow.arr(x => x * 2);
  assert.deepStrictEqual(dbl.second().run(['hello', 5]), ['hello', 10]);
});

test('parallel: both components', () => {
  const inc = SimpleArrow.arr(x => x + 1);
  const dbl = SimpleArrow.arr(x => x * 2);
  assert.deepStrictEqual(inc.parallel(dbl).run([5, 3]), [6, 6]);
});

test('fanout: same input, two outputs', () => {
  const inc = SimpleArrow.arr(x => x + 1);
  const dbl = SimpleArrow.arr(x => x * 2);
  assert.deepStrictEqual(inc.fanout(dbl).run(5), [6, 10]);
});

test('Kleisli: arr', () => {
  const r = KleisliArrow.arr(x => x + 1).run(41);
  assert.ok(r.ok);
  assert.equal(r.value, 42);
});

test('Kleisli: compose success', () => {
  const inc = KleisliArrow.arr(x => x + 1);
  const dbl = KleisliArrow.arr(x => x * 2);
  const r = inc.compose(dbl).run(5);
  assert.equal(r.value, 12);
});

test('Kleisli: compose failure', () => {
  const fail = new KleisliArrow(x => ({ ok: false }));
  const dbl = KleisliArrow.arr(x => x * 2);
  assert.ok(!fail.compose(dbl).run(5).ok);
});

test('accumulator: stateful', () => {
  const acc = accumulator(0);
  assert.equal(acc.run(5), 5);
  assert.equal(acc.run(3), 8);
  assert.equal(acc.run(2), 10);
});

test('loop: feedback', () => {
  const arr = new SimpleArrow(([input, state]) => [input + state, state]);
  const r = loop(arr, 10).run(5);
  assert.equal(r, 15);
});

console.log(`\n🎉🎉🎉 MODULE #160!!! Arrows tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
