import { strict as assert } from 'assert';
import {
  isSyntacticValue, generalizeRestricted,
  EVar, ENum, EBool, EStr, ELam, EApp, ELet, ERef, ECon, ETuple,
  TVar, TFun, tInt, tBool, Scheme
} from './value-restriction.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('variable is a value', () => assert.ok(isSyntacticValue(new EVar('x'))));
test('number is a value', () => assert.ok(isSyntacticValue(new ENum(42))));
test('lambda is a value', () => assert.ok(isSyntacticValue(new ELam('x', new EVar('x')))));
test('constructor with value args is a value', () => assert.ok(isSyntacticValue(new ECon('Just', [new ENum(42)]))));
test('tuple of values is a value', () => assert.ok(isSyntacticValue(new ETuple([new ENum(1), new EBool(true)]))));
test('application is NOT a value', () => assert.ok(!isSyntacticValue(new EApp(new EVar('f'), new EVar('x')))));
test('ref is NOT a value', () => assert.ok(!isSyntacticValue(new ERef(new ENum(42)))));

test('generalize value: λx.x → ∀a.(a → a)', () => {
  const type = new TFun(new TVar('a'), new TVar('a'));
  const scheme = generalizeRestricted(new Map(), type, true);
  assert.ok(scheme.vars.includes('a'));
});

test('generalize non-value: ref [] → no generalization', () => {
  const type = new TVar('a');
  const scheme = generalizeRestricted(new Map(), type, false);
  assert.equal(scheme.vars.length, 0); // No generalization!
});

test('generalize value respects env', () => {
  const env = new Map([['x', new Scheme([], new TVar('a'))]]);
  const type = new TFun(new TVar('a'), new TVar('b'));
  const scheme = generalizeRestricted(env, type, true);
  assert.ok(scheme.vars.includes('b'));   // b is free in type but not env → generalize
  assert.ok(!scheme.vars.includes('a'));  // a is in env → don't generalize
});

console.log(`\nValue restriction tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
