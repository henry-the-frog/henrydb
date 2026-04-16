import { strict as assert } from 'assert';
import { PropNet, addConstraint, mulConstraint } from './propagators.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('basic: forward propagation', () => {
  const net = new PropNet();
  const a = net.cell('a', 2), b = net.cell('b', 3), c = net.cell('c');
  addConstraint(net, a, b, c);
  net.run();
  assert.equal(net.get('c'), 5);
});

test('backward: infer a from sum and b', () => {
  const net = new PropNet();
  const a = net.cell('a'), b = net.cell('b', 3), c = net.cell('c', 5);
  addConstraint(net, a, b, c);
  net.run();
  assert.equal(net.get('a'), 2);
});

test('backward: infer b from sum and a', () => {
  const net = new PropNet();
  const a = net.cell('a', 2), b = net.cell('b'), c = net.cell('c', 5);
  addConstraint(net, a, b, c);
  net.run();
  assert.equal(net.get('b'), 3);
});

test('mul: forward', () => {
  const net = new PropNet();
  const a = net.cell('a', 4), b = net.cell('b', 5), c = net.cell('c');
  mulConstraint(net, a, b, c);
  net.run();
  assert.equal(net.get('c'), 20);
});

test('mul: backward', () => {
  const net = new PropNet();
  const a = net.cell('a'), b = net.cell('b', 5), c = net.cell('c', 20);
  mulConstraint(net, a, b, c);
  net.run();
  assert.equal(net.get('a'), 4);
});

test('chain: a+b=c, c+d=e', () => {
  const net = new PropNet();
  const a = net.cell('a', 1), b = net.cell('b', 2), c = net.cell('c');
  const d = net.cell('d', 3), e = net.cell('e');
  addConstraint(net, a, b, c);
  addConstraint(net, c, d, e);
  net.run();
  assert.equal(net.get('e'), 6);
});

test('contradiction: throws', () => {
  const net = new PropNet();
  const a = net.cell('a', 2), b = net.cell('b', 3), c = net.cell('c', 10); // 2+3≠10
  addConstraint(net, a, b, c);
  assert.throws(() => net.run(), /Contradiction/);
});

test('no info: stays null', () => {
  const net = new PropNet();
  net.cell('a');
  net.run();
  assert.equal(net.get('a'), null);
});

test('celsius/fahrenheit conversion', () => {
  const net = new PropNet();
  const c = net.cell('c', 100);
  const nine = net.cell('9', 9), five = net.cell('5', 5), thirtytwo = net.cell('32', 32);
  const cn = net.cell('cn'), f5 = net.cell('f5'), f = net.cell('f');
  mulConstraint(net, c, nine, cn); // c*9 = cn
  mulConstraint(net, f5, five, cn); // f5*5 = cn → f5 = cn/5
  addConstraint(net, f5, thirtytwo, f); // f = f5 + 32
  net.run();
  assert.equal(net.get('f'), 212);
});

console.log(`\nPropagator tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
