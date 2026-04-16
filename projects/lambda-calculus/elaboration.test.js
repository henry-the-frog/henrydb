import { strict as assert } from 'assert';
import {
  SVar, SLam, SApp, SLet, SNum, SBool, SIf, SAnn,
  tInt, tBool, TFun,
  elaborate
} from './elaboration.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('elaborate number: 42 → CNum(42) : Int', () => {
  const r = elaborate(new SNum(42));
  assert.equal(r.core.tag, 'CNum');
  assert.equal(r.type.tag, 'TInt');
});

test('elaborate identity: λx.x → CLam with inferred type', () => {
  const r = elaborate(new SLam('x', new SVar('x')));
  assert.equal(r.core.tag, 'CLam');
  assert.equal(r.type.tag, 'TFun');
});

test('elaborate application: (λx.x) 5 → type Int', () => {
  const r = elaborate(new SApp(new SLam('x', new SVar('x')), new SNum(5)));
  assert.equal(r.type.tag, 'TInt');
  assert.equal(r.errors.length, 0);
});

test('elaborate let: let x = 5 in x → Int', () => {
  const r = elaborate(new SLet('x', new SNum(5), new SVar('x')));
  assert.equal(r.type.tag, 'TInt');
  assert.ok(r.core.tag === 'CLet');
});

test('elaborate if: if true then 1 else 2 → Int', () => {
  const r = elaborate(new SIf(new SBool(true), new SNum(1), new SNum(2)));
  assert.equal(r.type.tag, 'TInt');
});

test('elaborate: if branch mismatch → error', () => {
  const r = elaborate(new SIf(new SBool(true), new SNum(1), new SBool(false)));
  assert.ok(r.errors.length > 0);
});

test('elaborate: annotation checked', () => {
  const r = elaborate(new SAnn(new SNum(42), tInt));
  assert.equal(r.type.tag, 'TInt');
  assert.equal(r.errors.length, 0);
});

test('elaborate: wrong annotation → error', () => {
  const r = elaborate(new SAnn(new SNum(42), tBool));
  assert.ok(r.errors.length > 0);
});

test('elaborate: application infers function type', () => {
  const r = elaborate(new SApp(new SLam('x', new SVar('x')), new SNum(5)));
  assert.equal(r.core.tag, 'CApp');
  assert.equal(r.type.tag, 'TInt'); // Result type inferred as Int
});

test('elaborate: const function', () => {
  const K = new SLam('x', new SLam('y', new SVar('x')));
  const r = elaborate(new SApp(new SApp(K, new SNum(1)), new SBool(true)));
  assert.equal(r.type.tag, 'TInt');
});

test('elaborate: nested let', () => {
  const r = elaborate(new SLet('x', new SNum(1), new SLet('y', new SNum(2), new SVar('y'))));
  assert.equal(r.type.tag, 'TInt');
});

test('elaborate: unbound variable → error', () => {
  const r = elaborate(new SVar('unknown'));
  assert.ok(r.errors.length > 0);
});

console.log(`\nElaboration tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
