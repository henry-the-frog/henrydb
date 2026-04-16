import { strict as assert } from 'assert';
import {
  tInt, tBool, tStr, TFun,
  EVar, ELam, EApp, ENum, EBool, EStr, EAnn, EIf, ELet,
  biInfer, biCheck
} from './bidirectional.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Infer mode
// ============================================================

test('infer: number → Int', () => {
  const { type, errors } = biInfer(new ENum(42));
  assert.equal(type.tag, 'TInt');
  assert.equal(errors.length, 0);
});

test('infer: boolean → Bool', () => {
  const { type } = biInfer(new EBool(true));
  assert.equal(type.tag, 'TBool');
});

test('infer: string → Str', () => {
  const { type } = biInfer(new EStr('hello'));
  assert.equal(type.tag, 'TStr');
});

test('infer: variable from env', () => {
  const { type, errors } = biInfer(new EVar('x'), new Map([['x', tInt]]));
  assert.equal(type.tag, 'TInt');
  assert.equal(errors.length, 0);
});

test('infer: unbound variable → error', () => {
  const { errors } = biInfer(new EVar('undefined'));
  assert.ok(errors.length > 0);
});

test('infer: application', () => {
  const env = new Map([['f', new TFun(tInt, tBool)]]);
  const { type } = biInfer(new EApp(new EVar('f'), new ENum(42)), env);
  assert.equal(type.tag, 'TBool');
});

test('infer: annotation', () => {
  const { type } = biInfer(new EAnn(new ENum(42), tInt));
  assert.equal(type.tag, 'TInt');
});

test('infer: let binding', () => {
  const { type } = biInfer(new ELet('x', new ENum(5), new EVar('x')));
  assert.equal(type.tag, 'TInt');
});

test('infer: if expression', () => {
  const { type, errors } = biInfer(new EIf(new EBool(true), new ENum(1), new ENum(2)));
  assert.equal(type.tag, 'TInt');
  assert.equal(errors.length, 0);
});

// ============================================================
// Check mode
// ============================================================

test('check: lambda against function type', () => {
  const { errors } = biCheck(
    new ELam('x', new EVar('x')),
    new TFun(tInt, tInt));
  assert.equal(errors.length, 0);
});

test('check: lambda body type mismatch → error', () => {
  const { errors } = biCheck(
    new ELam('x', new EBool(true)),
    new TFun(tInt, tInt));
  assert.ok(errors.length > 0);
});

test('check: nested lambda', () => {
  const { errors } = biCheck(
    new ELam('x', new ELam('y', new EVar('x'))),
    new TFun(tInt, new TFun(tBool, tInt)));
  assert.equal(errors.length, 0);
});

test('check: subsumption (infer matches check)', () => {
  const { errors } = biCheck(new ENum(42), tInt);
  assert.equal(errors.length, 0);
});

test('check: type mismatch → error', () => {
  const { errors } = biCheck(new ENum(42), tBool);
  assert.ok(errors.length > 0);
});

// ============================================================
// Key advantage: lambda without annotation
// ============================================================

test('key: cannot infer lambda (needs annotation)', () => {
  const { errors } = biInfer(new ELam('x', new EVar('x')));
  assert.ok(errors.length > 0); // Cannot infer
});

test('key: CAN check lambda (annotation not needed)', () => {
  const { errors } = biCheck(
    new ELam('x', new EVar('x')),
    new TFun(tInt, tInt));
  assert.equal(errors.length, 0); // Works!
});

test('key: annotated lambda CAN be inferred', () => {
  const { type, errors } = biInfer(
    new EAnn(new ELam('x', new EVar('x')), new TFun(tInt, tInt)));
  assert.equal(type.toString(), '(Int → Int)');
  assert.equal(errors.length, 0);
});

// ============================================================
// Report
// ============================================================

console.log(`\nBidirectional type checking tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
