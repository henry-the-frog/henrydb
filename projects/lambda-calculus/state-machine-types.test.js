import { strict as assert } from 'assert';
import { StateMachine, TypedHandle, FileHandle, HttpRequest, TCPConnection } from './state-machine-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// File handle
test('file: open → read → close valid', () => {
  const r = FileHandle.validateSequence(['open', 'read', 'read', 'close']);
  assert.ok(r.valid);
  assert.equal(r.finalState, 'Closed');
});

test('file: read from Closed → ERROR', () => {
  const r = FileHandle.validateSequence(['read']);
  assert.ok(!r.valid);
});

test('file: open → close → read → ERROR', () => {
  const r = FileHandle.validateSequence(['open', 'close', 'read']);
  assert.ok(!r.valid);
});

// HTTP request
test('http: setMethod → setUrl → send valid', () => {
  const r = HttpRequest.validateSequence(['setMethod', 'setUrl', 'send']);
  assert.ok(r.valid);
  assert.equal(r.finalState, 'Sent');
});

test('http: send without URL → ERROR', () => {
  const r = HttpRequest.validateSequence(['setMethod', 'send']);
  assert.ok(!r.valid);
});

test('http: with headers', () => {
  const r = HttpRequest.validateSequence(['setMethod', 'setUrl', 'addHeader', 'addHeader', 'send']);
  assert.ok(r.valid);
});

// TypedHandle
test('typed handle: valid operations', () => {
  let h = new TypedHandle(FileHandle);
  h = h.do('open');
  h = h.do('read');
  h = h.do('close');
  assert.equal(h.state, 'Closed');
});

test('typed handle: invalid operation throws', () => {
  const h = new TypedHandle(FileHandle);
  assert.throws(() => h.do('read'), /Invalid/);
});

test('typed handle: available actions', () => {
  const h = new TypedHandle(FileHandle);
  assert.deepStrictEqual(h.availableActions(), ['open']);
});

test('typed handle: can check', () => {
  const h = new TypedHandle(FileHandle);
  assert.ok(h.can('open'));
  assert.ok(!h.can('read'));
});

// Reachability
test('file handle: all states reachable', () => {
  assert.ok(FileHandle.isFullyReachable());
});

test('TCP: all states reachable', () => {
  assert.ok(TCPConnection.isFullyReachable());
});

console.log(`\nState machine types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
