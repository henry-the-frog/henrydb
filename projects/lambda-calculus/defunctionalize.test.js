import { strict as assert } from 'assert';
import {
  Defunctionalizer, evalWithApply,
  snum, svar, slam, sapp, slet, sprim
} from './defunctionalize.js';

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
// Defunctionalization transform
// ============================================================

test('number: no transformation', () => {
  const defunc = new Defunctionalizer();
  const { program } = defunc.transform(snum(42));
  assert.equal(program.tag, 'TNum');
  assert.equal(program.n, 42);
});

test('lambda becomes closure', () => {
  const defunc = new Defunctionalizer();
  const { program } = defunc.transform(slam('x', svar('x')));
  assert.equal(program.tag, 'TClos');
});

test('application becomes apply', () => {
  const defunc = new Defunctionalizer();
  const { program } = defunc.transform(sapp(slam('x', svar('x')), snum(5)));
  assert.equal(program.tag, 'TApply');
});

test('apply cases generated', () => {
  const defunc = new Defunctionalizer();
  defunc.transform(slam('x', svar('x')));
  assert.equal(defunc.applyCases.length, 1);
  assert.equal(defunc.applyCases[0].param, 'x');
});

test('multiple lambdas get different IDs', () => {
  const defunc = new Defunctionalizer();
  defunc.transform(slet('f', slam('x', svar('x')), slam('y', svar('y'))));
  assert.equal(defunc.applyCases.length, 2);
  assert.notEqual(defunc.applyCases[0].id, defunc.applyCases[1].id);
});

// ============================================================
// Evaluation of defunctionalized programs
// ============================================================

test('eval: identity (λx.x) 42 → 42', () => {
  const result = evalWithApply(sapp(slam('x', svar('x')), snum(42)));
  assert.equal(result, 42);
});

test('eval: constant (λx.5) 99 → 5', () => {
  const result = evalWithApply(sapp(slam('x', snum(5)), snum(99)));
  assert.equal(result, 5);
});

test('eval: arithmetic (λx. x + 1) 41 → 42', () => {
  const result = evalWithApply(sapp(slam('x', sprim('+', svar('x'), snum(1))), snum(41)));
  assert.equal(result, 42);
});

test('eval: closure captures free var: let y=10 in (λx. x + y) 5 → 15', () => {
  const result = evalWithApply(
    slet('y', snum(10),
      sapp(slam('x', sprim('+', svar('x'), svar('y')), ['y']), snum(5))));
  assert.equal(result, 15);
});

test('eval: let binding: let f = λx. x*2 in f(21) → 42', () => {
  const result = evalWithApply(
    slet('f', slam('x', sprim('*', svar('x'), snum(2))),
      sapp(svar('f'), snum(21))));
  assert.equal(result, 42);
});

test('eval: nested application: (λf. f 5) (λx. x + 1) → 6', () => {
  const result = evalWithApply(
    sapp(
      slam('f', sapp(svar('f'), snum(5))),
      slam('x', sprim('+', svar('x'), snum(1)))
    ));
  assert.equal(result, 6);
});

// ============================================================
// Code generation
// ============================================================

test('generateApply produces valid code', () => {
  const defunc = new Defunctionalizer();
  defunc.transform(slam('x', sprim('+', svar('x'), snum(1))));
  const code = defunc.generateApply();
  assert.ok(code.includes('function apply'));
  assert.ok(code.includes('switch'));
  assert.ok(code.includes('case 0'));
});

// ============================================================
// Report
// ============================================================

console.log(`\nDefunctionalization tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
