import { strict as assert } from 'assert';
import {
  tInt, tBool, tStr, TFun,
  EVar, ELam, EApp, ENum, EStr, ELet,
  Monomorphizer, specialize, typeKey
} from './monomorphize.js';

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
// Type key generation
// ============================================================

test('typeKey: Int', () => assert.equal(typeKey(tInt), 'Int'));
test('typeKey: Int → Bool', () => assert.equal(typeKey(new TFun(tInt, tBool)), '(Int->Bool)'));
test('typeKey: nested function', () => {
  assert.equal(typeKey(new TFun(tInt, new TFun(tStr, tBool))), '(Int->(Str->Bool))');
});

// ============================================================
// Specialize function
// ============================================================

test('specialize: id at Int and Str', () => {
  const body = new ELam('x', new EVar('x'));
  const specs = specialize('id', body, [
    { argType: tInt, name: 'id_Int' },
    { argType: tStr, name: 'id_Str' },
  ]);
  assert.equal(specs.length, 2);
  assert.equal(specs[0].name, 'id_Int');
  assert.equal(specs[1].name, 'id_Str');
});

// ============================================================
// Monomorphizer
// ============================================================

test('monomorphize: id used at Int', () => {
  const app = new EApp(new EVar('id'), new ENum(42));
  const callTypes = new Map([[app, tInt]]);
  
  const mono = new Monomorphizer();
  const result = mono.monomorphize(app, callTypes);
  assert.equal(result.generatedCount, 1);
  assert.ok(result.specializations[0].name.includes('Int'));
});

test('monomorphize: id used at Int and Str', () => {
  const app1 = new EApp(new EVar('id'), new ENum(42));
  const app2 = new EApp(new EVar('id'), new EStr('hello'));
  const body = new ELet('a', app1, app2);
  
  const callTypes = new Map([[app1, tInt], [app2, tStr]]);
  
  const mono = new Monomorphizer();
  const result = mono.monomorphize(body, callTypes);
  assert.equal(result.generatedCount, 2);
});

test('monomorphize: transforms call sites', () => {
  const app = new EApp(new EVar('id'), new ENum(42));
  const callTypes = new Map([[app, tInt]]);
  
  const mono = new Monomorphizer();
  const result = mono.monomorphize(app, callTypes);
  // The transformed program should reference id_Int, not id
  assert.equal(result.program.fn.name, 'id_Int');
});

test('monomorphize: no polymorphic calls → no specializations', () => {
  const expr = new ENum(42);
  const mono = new Monomorphizer();
  const result = mono.monomorphize(expr, new Map());
  assert.equal(result.generatedCount, 0);
});

test('monomorphize: same type → same specialization', () => {
  const app1 = new EApp(new EVar('id'), new ENum(1));
  const app2 = new EApp(new EVar('id'), new ENum(2));
  const body = new ELet('a', app1, app2);
  
  const callTypes = new Map([[app1, tInt], [app2, tInt]]);
  
  const mono = new Monomorphizer();
  const result = mono.monomorphize(body, callTypes);
  // Same type should generate only 1 specialization
  assert.equal(result.generatedCount, 1);
});

test('monomorphize: function type specialization', () => {
  const app = new EApp(new EVar('apply'), new EVar('f'));
  const callTypes = new Map([[app, new TFun(tInt, tBool)]]);
  
  const mono = new Monomorphizer();
  const result = mono.monomorphize(app, callTypes);
  assert.equal(result.generatedCount, 1);
});

// ============================================================
// Report
// ============================================================

console.log(`\nMonomorphization tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
