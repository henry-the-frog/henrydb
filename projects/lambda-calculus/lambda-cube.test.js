import { strict as assert } from 'assert';
import {
  POLYMORPHISM, TYPE_OPERATORS, DEPENDENT_TYPES,
  STLC, SystemF, Fomega, LambdaP, SystemFomega, CoC,
  lambdaCube, whatCanExpress, subsystemRelations, findSystem
} from './lambda-cube.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('8 systems in the cube', () => assert.equal(lambdaCube.length, 8));

test('STLC has no features', () => assert.equal(STLC.features.size, 0));
test('System F has polymorphism', () => assert.ok(SystemF.has(POLYMORPHISM)));
test('CoC has all features', () => {
  assert.ok(CoC.has(POLYMORPHISM));
  assert.ok(CoC.has(TYPE_OPERATORS));
  assert.ok(CoC.has(DEPENDENT_TYPES));
});

test('STLC ⊆ System F', () => assert.ok(STLC.isSubsystemOf(SystemF)));
test('STLC ⊆ CoC', () => assert.ok(STLC.isSubsystemOf(CoC)));
test('System F !⊆ STLC', () => assert.ok(!SystemF.isSubsystemOf(STLC)));
test('System F ⊆ Fω', () => assert.ok(SystemF.isSubsystemOf(SystemFomega)));

test('findSystem: polymorphism only = System F', () => {
  assert.equal(findSystem([POLYMORPHISM]).name, 'λ2');
});

test('findSystem: all = CoC', () => {
  assert.equal(findSystem([POLYMORPHISM, TYPE_OPERATORS, DEPENDENT_TYPES]).name, 'λ2ωP');
});

test('whatCanExpress: STLC is monomorphic', () => {
  assert.ok(whatCanExpress(STLC).some(a => a.includes('monomorphic')));
});

test('subsystemRelations: non-empty', () => {
  const relations = subsystemRelations();
  assert.ok(relations.length > 0);
  assert.ok(relations.some(r => r.includes('λ→ ⊆ λ2')));
});

console.log(`\nLambda cube tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
