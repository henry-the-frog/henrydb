import { strict as assert } from 'assert';
import { IxState, Protocol, protocolAction, bracket } from './typed-state.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('pure: value without state change', () => {
  const r = IxState.pure(42).exec('init');
  assert.equal(r.value, 42);
  assert.equal(r.state, 'init');
});

test('get: read state', () => {
  const r = IxState.get().exec('hello');
  assert.equal(r.value, 'hello');
});

test('put: replace state', () => {
  const r = IxState.put('new').exec('old');
  assert.equal(r.state, 'new');
});

test('modify: transform state', () => {
  const r = IxState.modify(n => n + 1).exec(41);
  assert.equal(r.state, 42);
});

test('chain: sequence operations', () => {
  const comp = IxState.get().chain(n => IxState.put(n * 2).chain(() => IxState.get()));
  const r = comp.exec(21);
  assert.equal(r.value, 42);
});

test('map: transform result', () => {
  const r = IxState.pure(21).map(x => x * 2).exec('s');
  assert.equal(r.value, 42);
});

// Protocol
const fileProto = new Protocol([
  { from: 'closed', action: 'open', to: 'open' },
  { from: 'open', action: 'read', to: 'open' },
  { from: 'open', action: 'close', to: 'closed' },
]);

test('protocol: valid transition', () => {
  const r = protocolAction(fileProto, 'open').exec('closed');
  assert.equal(r.state, 'open');
});

test('protocol: invalid transition → error', () => {
  assert.throws(() => protocolAction(fileProto, 'read').exec('closed'), /Invalid/);
});

test('protocol: chained transitions', () => {
  const comp = protocolAction(fileProto, 'open')
    .chain(() => protocolAction(fileProto, 'read'))
    .chain(() => protocolAction(fileProto, 'close'));
  const r = comp.exec('closed');
  assert.equal(r.state, 'closed');
});

test('bracket: acquire → use → release', () => {
  const acquire = IxState.put('acquired').map(() => 'resource');
  const use = r => IxState.pure(`used ${r}`);
  const release = r => IxState.put('released');
  const r = bracket(acquire, use, release).exec('init');
  assert.equal(r.value, 'used resource');
  assert.equal(r.state, 'released');
});

console.log(`\nTyped state tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
