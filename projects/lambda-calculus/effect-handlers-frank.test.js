import { strict as assert } from 'assert';
import { perform, stateHandler, exnHandler, choiceHandler } from './effect-handlers-frank.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('state: get initial', () => {
  const h = stateHandler(42);
  const [v] = h.handle(() => perform('get'));
  assert.equal(v, 42);
});

test('state: put changes state', () => {
  const h = stateHandler(0);
  const [v, s] = h.handle(() => { perform('put', 99); return 'done'; });
  assert.equal(s, 99);
});

test('exception: no error', () => {
  const r = exnHandler().handle(() => 42);
  assert.ok(r.ok);
  assert.equal(r.value, 42);
});

test('exception: raise', () => {
  const r = exnHandler().handle(() => perform('raise', 'boom'));
  assert.ok(!r.ok);
  assert.equal(r.error, 'boom');
});

test('choice: no choice → single result', () => {
  const r = choiceHandler().handle(() => 42);
  assert.deepStrictEqual(r, [42]);
});

test('choice: choose from options', () => {
  const r = choiceHandler().handle(() => perform('choose', [1, 2, 3]));
  assert.deepStrictEqual(r, [1, 2, 3]);
});

test('state: initial state preserved', () => {
  const [_, s] = stateHandler(10).handle(() => 'done');
  assert.equal(s, 10);
});

test('exception: return value', () => {
  const r = exnHandler().handle(() => 'ok');
  assert.equal(r.value, 'ok');
});

test('state: single put', () => {
  const h = stateHandler(0);
  const [v, s] = h.handle(() => { perform('put', 2); return 'done'; });
  assert.equal(s, 2);
});

test('exception handler: value wrapping', () => {
  const r = exnHandler().handle(() => ({ data: [1, 2, 3] }));
  assert.deepStrictEqual(r.value.data, [1, 2, 3]);
});

console.log(`\nEffect handlers (Frank) tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
