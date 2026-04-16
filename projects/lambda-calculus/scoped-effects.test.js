import { strict as assert } from 'assert';
import { withHandler, nested, ask, local, stateHandler } from './scoped-effects.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('withHandler: success', () => {
  const r = withHandler('test', { greet: n => `hi ${n}` }, h => h.greet('world'));
  assert.equal(r.value, 'hi world');
});

test('withHandler: returns ok', () => {
  const r = withHandler('test', {}, h => 42);
  assert.ok(r.ok);
  assert.equal(r.value, 42);
});

test('nested: inner shadows outer', () => {
  const r = nested(
    { val: () => 'outer' },
    { val: () => 'inner' },
    h => h.val()
  );
  assert.equal(r.value, 'inner');
});

test('ask: reader effect', () => {
  const handlers = { ask: () => 42 };
  assert.equal(ask(handlers), 42);
});

test('local: modify reader', () => {
  const handlers = { ask: () => 10 };
  const result = local(handlers, x => x * 2, h => h.ask());
  assert.equal(result, 20);
});

test('stateHandler: get initial', () => {
  const s = stateHandler(0);
  assert.equal(s.get(), 0);
});

test('stateHandler: put', () => {
  const s = stateHandler(0);
  s.put(42);
  assert.equal(s.get(), 42);
});

test('stateHandler: modify', () => {
  const s = stateHandler(10);
  s.modify(x => x + 5);
  assert.equal(s.get(), 15);
});

test('stateHandler: sequence', () => {
  const s = stateHandler(0);
  s.modify(x => x + 1);
  s.modify(x => x + 1);
  s.modify(x => x + 1);
  assert.equal(s.get(), 3);
});

test('withHandler: returns value', () => {
  const r = withHandler('math', { add: (a, b) => a + b }, h => h.add(2, 3));
  assert.equal(r.value, 5);
});

console.log(`\n🎉 MODULE #165! Scoped effects tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
