import { strict as assert } from 'assert';
import { EVal, EBool, EVar, EThunk, EForce, ERet, ETo, ELam, EApp, CBPVMachine } from './cbpv.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('return 42', () => {
  const m = new CBPVMachine();
  const r = m.run(new ERet(new EVal(42)));
  assert.equal(r.value.n, 42);
});

test('return bool', () => {
  const m = new CBPVMachine();
  assert.equal(m.run(new ERet(new EBool(true))).value.v, true);
});

test('to: bind return 5 to x, return x', () => {
  const m = new CBPVMachine();
  const expr = new ETo(new ERet(new EVal(5)), 'x', new ERet(new EVar('x')));
  assert.equal(m.run(expr).value.n, 5);
});

test('thunk/force: freeze then run', () => {
  const m = new CBPVMachine();
  const comp = new ERet(new EVal(42));
  const thunked = new EThunk(comp);
  const result = m.run(new EForce(thunked));
  assert.equal(result.value.n, 42);
});

test('lambda: (λx. return x) 5', () => {
  const m = new CBPVMachine();
  const expr = new EApp(new ELam('x', new ERet(new EVar('x'))), new EVal(5));
  assert.equal(m.run(expr).value.n, 5);
});

test('nested to: bind chain', () => {
  const m = new CBPVMachine();
  const expr = new ETo(
    new ERet(new EVal(1)),
    'x',
    new ETo(new ERet(new EVal(2)), 'y', new ERet(new EVar('y')))
  );
  assert.equal(m.run(expr).value.n, 2);
});

test('thunk in variable: store and force', () => {
  const m = new CBPVMachine();
  const comp = new ERet(new EVal(99));
  const expr = new ETo(
    new ERet(new EThunk(comp)),
    'th',
    new EForce(new EVar('th'))
  );
  assert.equal(m.run(expr).value.n, 99);
});

test('higher-order: pass function as thunk', () => {
  const m = new CBPVMachine();
  const fn = new ELam('n', new ERet(new EVar('n')));
  const thunked = new EThunk(fn);
  const expr = new ETo(
    new ERet(thunked),
    'f',
    new EApp(new EForce(new EVar('f')), new EVal(77))
  );
  assert.equal(m.run(expr).value.n, 77);
});

test('force non-thunk → error', () => {
  const m = new CBPVMachine();
  assert.throws(() => m.run(new EForce(new EVal(42))), /not a thunk/);
});

test('step counting works', () => {
  const m = new CBPVMachine();
  m.run(new ERet(new EVal(1)));
  assert.ok(m.steps > 0);
});

console.log(`\nCBPV tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
