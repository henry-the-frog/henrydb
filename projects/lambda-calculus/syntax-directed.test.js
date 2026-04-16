import { strict as assert } from 'assert';
import { translate, Num, Var, Add, Mul, Neg, Let, If, Call } from './syntax-directed.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('num: const', () => assert.ok(translate(Num(42)).toString().includes('const 42')));
test('add: add op', () => assert.ok(translate(Add(Num(2), Num(3))).toString().includes('add')));
test('mul: mul op', () => assert.ok(translate(Mul(Num(4), Num(5))).toString().includes('mul')));
test('neg: neg op', () => assert.ok(translate(Neg(Num(7))).toString().includes('neg')));
test('let: assignment', () => assert.ok(translate(Let('x', Num(5), Var('x'))).toString().includes('x =')));
test('if: branches', () => {
  const ir = translate(If(Var('b'), Num(1), Num(2)));
  assert.ok(ir.toString().includes('br'));
});
test('call: function call', () => assert.ok(translate(Call('f', [Num(1)])).toString().includes('call f')));
test('nested: (2+3)*4', () => {
  const ir = translate(Mul(Add(Num(2), Num(3)), Num(4)));
  assert.ok(ir.toString().includes('add'));
  assert.ok(ir.toString().includes('mul'));
});
test('target: assigns to target', () => {
  const ir = translate(Num(42), 'result');
  assert.ok(ir.toString().includes('result ='));
});
test('ret: returns result', () => assert.ok(translate(Num(42)).toString().includes('ret')));

console.log(`\n🎉🎉🎉 MODULE #175!!! Syntax-directed translation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
