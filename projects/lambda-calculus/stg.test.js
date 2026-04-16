import { strict as assert } from 'assert';
import { EVar, ELam, EApp, ECon, ELet, ENum, EPrim, STGMachine } from './stg.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('number literal', () => {
  const m = new STGMachine();
  const r = m.run(new ENum(42));
  assert.equal(r.value, 42);
});

test('lambda application', () => {
  const m = new STGMachine();
  const expr = new EApp(new ELam(['x'], new EVar('x')), [new ENum(42)]);
  assert.equal(m.run(expr).value, 42);
});

test('primop: 3 + 4', () => {
  const m = new STGMachine();
  assert.equal(m.run(new EPrim('+', [new ENum(3), new ENum(4)])).value, 7);
});

test('primop: 10 - 3', () => {
  const m = new STGMachine();
  assert.equal(m.run(new EPrim('-', [new ENum(10), new ENum(3)])).value, 7);
});

test('let: let x = 5 in x + 1', () => {
  const m = new STGMachine();
  const expr = new ELet([['x', new ENum(5)]], new EPrim('+', [new EVar('x'), new ENum(1)]));
  assert.equal(m.run(expr).value, 6);
});

test('thunk sharing: let x = expensive in x + x', () => {
  const m = new STGMachine();
  const expr = new ELet(
    [['x', new EPrim('+', [new ENum(100), new ENum(200)])]],
    new EPrim('+', [new EVar('x'), new EVar('x')])
  );
  assert.equal(m.run(expr).value, 600);
  assert.ok(m.updates > 0); // Thunk was updated
});

test('constructor', () => {
  const m = new STGMachine();
  const r = m.run(new ECon('Pair', [new ENum(1), new ENum(2)]));
  assert.equal(r.ctag, 'Pair');
});

test('nested let', () => {
  const m = new STGMachine();
  const expr = new ELet(
    [['x', new ENum(1)]],
    new ELet([['y', new ENum(2)]], new EPrim('+', [new EVar('x'), new EVar('y')]))
  );
  assert.equal(m.run(expr).value, 3);
});

test('higher-order: apply f x', () => {
  const m = new STGMachine();
  const expr = new EApp(
    new ELam(['f', 'x'], new EApp(new EVar('f'), [new EVar('x')])),
    [new ELam(['n'], new EPrim('+', [new EVar('n'), new ENum(1)])), new ENum(41)]
  );
  assert.equal(m.run(expr).value, 42);
});

test('heap grows', () => {
  const m = new STGMachine();
  m.run(new ELet([['x', new ENum(1)], ['y', new ENum(2)]], new EPrim('+', [new EVar('x'), new EVar('y')])));
  assert.ok(m.heap.size > 0);
});

test('step counting', () => {
  const m = new STGMachine();
  m.run(new EPrim('+', [new ENum(1), new ENum(2)]));
  assert.ok(m.steps > 0);
});

console.log(`\nSTG machine tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
