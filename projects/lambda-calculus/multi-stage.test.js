import { strict as assert } from 'assert';
import { Code, CNum, CVar, CAdd, CMul, CLam, CApp, quote, splice, run, powerStaged } from './multi-stage.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('quote: number → Code', () => assert.equal(quote(42).ast.n, 42));
test('splice: extract AST', () => assert.equal(splice(new Code(new CNum(5))).n, 5));
test('run: CNum', () => assert.equal(run(new Code(new CNum(42))), 42));
test('run: CAdd', () => assert.equal(run(new Code(new CAdd(new CNum(2), new CNum(3)))), 5));
test('run: CMul', () => assert.equal(run(new Code(new CMul(new CNum(4), new CNum(5)))), 20));
test('run: CLam + CApp', () => {
  const code = new Code(new CApp(new CLam('x', new CAdd(new CVar('x'), new CNum(1))), new CNum(41)));
  assert.equal(run(code), 42);
});

test('powerStaged: x^0 = 1', () => assert.equal(run(powerStaged(0)), 1));
test('powerStaged: x^1 = x', () => assert.equal(run(powerStaged(1), new Map([['x', 5]])), 5));
test('powerStaged: x^3 generates code', () => {
  const code = powerStaged(3);
  assert.equal(run(code, new Map([['x', 2]])), 8);
});
test('powerStaged: x^4 = 16', () => assert.equal(run(powerStaged(4), new Map([['x', 2]])), 16));

console.log(`\nMulti-stage programming tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
