import { strict as assert } from 'assert';
import {
  TVar, TFun, TCon, tInt, tBool,
  evar, elam, eapp, elet, eint, ebool,
  infer, resetFresh
} from './hindley-milner.js';
import { inferByConstraints, ConstraintGenerator } from './constraint-inference.js';

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
// Basic constraint inference
// ============================================================

test('literal: Int', () => {
  const { type } = inferByConstraints(eint);
  assert.equal(type.toString(), 'Int');
});

test('literal: Bool', () => {
  const { type } = inferByConstraints(ebool);
  assert.equal(type.toString(), 'Bool');
});

test('identity: λx.x', () => {
  const { type } = inferByConstraints(elam('x', evar('x')));
  assert.equal(type.tag, 'TFun');
  assert.equal(type.param.toString(), type.ret.toString()); // a → a
});

test('constant: λx.42', () => {
  const { type } = inferByConstraints(elam('x', eint));
  assert.equal(type.tag, 'TFun');
  assert.equal(type.ret.toString(), 'Int');
});

test('application: (λx.x) 42', () => {
  const { type } = inferByConstraints(eapp(elam('x', evar('x')), eint));
  assert.equal(type.toString(), 'Int');
});

// ============================================================
// Constraint generation
// ============================================================

test('application generates constraint', () => {
  resetFresh();
  const gen = new ConstraintGenerator();
  gen.generate(eapp(elam('x', evar('x')), eint));
  assert.ok(gen.getConstraints().length >= 1);
  assert.equal(gen.getConstraints()[0].tag, 'CEq');
});

test('nested application generates multiple constraints', () => {
  resetFresh();
  const gen = new ConstraintGenerator();
  gen.generate(eapp(eapp(elam('f', elam('x', eapp(evar('f'), evar('x')))), elam('y', evar('y'))), eint));
  assert.ok(gen.getConstraints().length >= 2);
});

// ============================================================
// Let-polymorphism
// ============================================================

test('let-polymorphism: let id = λx.x in id 42', () => {
  const { type } = inferByConstraints(elet('id', elam('x', evar('x')), eapp(evar('id'), eint)));
  assert.equal(type.toString(), 'Int');
});

test('let-polymorphism: id at two types', () => {
  const { type } = inferByConstraints(
    elet('id', elam('x', evar('x')),
      elet('a', eapp(evar('id'), eint),
        eapp(evar('id'), ebool))));
  assert.equal(type.toString(), 'Bool');
});

// ============================================================
// Equivalence with Algorithm W
// ============================================================

test('equivalence: (λx.x) 42 — same result as W', () => {
  const expr = eapp(elam('x', evar('x')), eint);
  
  resetFresh();
  const wResult = infer(expr);
  const wType = wResult.subst.apply(wResult.type);
  
  const cResult = inferByConstraints(expr);
  
  assert.equal(wType.toString(), cResult.type.toString());
});

test('equivalence: let id = λx.x in id 42 — same as W', () => {
  const expr = elet('id', elam('x', evar('x')), eapp(evar('id'), eint));
  
  resetFresh();
  const wResult = infer(expr);
  const wType = wResult.subst.apply(wResult.type);
  
  const cResult = inferByConstraints(expr);
  
  assert.equal(wType.toString(), cResult.type.toString());
});

test('equivalence: constant function — same as W', () => {
  const expr = elet('const', elam('x', elam('y', evar('x'))),
    eapp(eapp(evar('const'), eint), ebool));
  
  resetFresh();
  const wResult = infer(expr);
  const wType = wResult.subst.apply(wResult.type);
  
  const cResult = inferByConstraints(expr);
  
  assert.equal(wType.toString(), cResult.type.toString());
});

// ============================================================
// Error cases
// ============================================================

test('unbound variable', () => {
  assert.throws(() => inferByConstraints(evar('undefined')), /Unbound/);
});

test('type error in constraint: (42 true)', () => {
  const { errors } = inferByConstraints(eapp(eint, ebool));
  // Should have a unification error
  assert.ok(errors.length > 0);
});

// ============================================================
// Complex programs
// ============================================================

test('compose function', () => {
  const compose = elam('f', elam('g', elam('x', eapp(evar('f'), eapp(evar('g'), evar('x'))))));
  const { type } = inferByConstraints(compose);
  assert.equal(type.tag, 'TFun');
});

test('S combinator', () => {
  const S = elam('f', elam('g', elam('x',
    eapp(eapp(evar('f'), evar('x')), eapp(evar('g'), evar('x'))))));
  const { type } = inferByConstraints(S);
  assert.equal(type.tag, 'TFun');
});

// ============================================================
// Report
// ============================================================

console.log(`\nConstraint inference tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
