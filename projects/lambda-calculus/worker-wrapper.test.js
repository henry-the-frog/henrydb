import { strict as assert } from 'assert';
import { Fun, Var, Num, Call, BinOp, Box, Unbox, workerWrapper, countBoxOps } from './worker-wrapper.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('no strict params → no worker', () => {
  const fn = new Fun('f', ['x'], new Var('x'));
  const { worker } = workerWrapper(fn, []);
  assert.equal(worker, null);
});

test('strict param → creates worker', () => {
  const fn = new Fun('add1', ['x'], new BinOp('+', new Var('x'), new Num(1)));
  const { wrapper, worker } = workerWrapper(fn, [0]);
  assert.ok(worker);
  assert.equal(worker.name, 'add1_worker');
});

test('worker has unboxed param name', () => {
  const fn = new Fun('f', ['x'], new Var('x'));
  const { worker } = workerWrapper(fn, [0]);
  assert.equal(worker.params[0], 'x#');
});

test('wrapper calls worker with unbox', () => {
  const fn = new Fun('f', ['x'], new Var('x'));
  const { wrapper } = workerWrapper(fn, [0]);
  assert.equal(wrapper.body.tag, 'Call');
  assert.equal(wrapper.body.args[0].tag, 'Unbox');
});

test('multiple strict params', () => {
  const fn = new Fun('add', ['x', 'y'], new BinOp('+', new Var('x'), new Var('y')));
  const { worker } = workerWrapper(fn, [0, 1]);
  assert.equal(worker.params[0], 'x#');
  assert.equal(worker.params[1], 'y#');
});

test('non-strict param preserved', () => {
  const fn = new Fun('f', ['x', 'y'], new BinOp('+', new Var('x'), new Var('y')));
  const { worker } = workerWrapper(fn, [0]); // Only x is strict
  assert.equal(worker.params[0], 'x#');
  assert.equal(worker.params[1], 'y'); // Not unboxed
});

test('countBoxOps: Box and Unbox', () => {
  const expr = new Box(new Unbox(new Var('x')));
  const { boxes, unboxes } = countBoxOps(expr);
  assert.equal(boxes, 1);
  assert.equal(unboxes, 1);
});

test('countBoxOps: no ops', () => {
  const { boxes } = countBoxOps(new Var('x'));
  assert.equal(boxes, 0);
});

test('worker body uses unboxed name', () => {
  const fn = new Fun('f', ['x'], new BinOp('+', new Var('x'), new Num(1)));
  const { worker } = workerWrapper(fn, [0]);
  // Worker body should reference x# not x
  assert.equal(worker.body.left.name, 'x#');
});

test('wrapper name matches original', () => {
  const fn = new Fun('myFn', ['a'], new Var('a'));
  const { wrapper } = workerWrapper(fn, [0]);
  assert.equal(wrapper.name, 'myFn');
});

console.log(`\nWorker-wrapper tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
