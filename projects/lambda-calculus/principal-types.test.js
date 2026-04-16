import { strict as assert } from 'assert';
import { infer, typeStr, hasVars, ENum, EBool, EVar, ELam, EApp } from './principal-types.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('num → Int', () => assert.equal(typeStr(infer(ENum(42))), 'Int'));
test('bool → Bool', () => assert.equal(typeStr(infer(EBool(true))), 'Bool'));
test('identity → a→a', () => { const t = infer(ELam('x', EVar('x'))); assert.equal(t.tag, 'TFun'); assert.equal(typeStr(t.param), typeStr(t.ret)); });
test('const → a→b→a', () => { const t = infer(ELam('x', ELam('y', EVar('x')))); assert.equal(t.tag, 'TFun'); });
test('app id 42 → Int', () => assert.equal(typeStr(infer(EApp(ELam('x', EVar('x')), ENum(42)))), 'Int'));
test('id has type vars', () => assert.ok(hasVars(infer(ELam('x', EVar('x'))))));
test('42 has no type vars', () => assert.ok(!hasVars(infer(ENum(42)))));
test('unbound → error', () => assert.throws(() => infer(EVar('z')), /Unbound/));
test('app non-fun → error', () => assert.throws(() => infer(EApp(ENum(1), ENum(2))), /Unify/));
test('nested app', () => {
  const t = infer(EApp(ELam('f', EApp(EVar('f'), ENum(1))), ELam('x', EVar('x'))));
  assert.equal(typeStr(t), 'Int');
});

console.log(`\nPrincipal types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
