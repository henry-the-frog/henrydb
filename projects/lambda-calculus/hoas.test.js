import { strict as assert } from 'assert';
import { HOAS, evalHOAS, sizeHOAS } from './hoas.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('num', () => assert.equal(evalHOAS(HOAS.num(42)), 42));
test('add', () => assert.equal(evalHOAS(HOAS.add(HOAS.num(2), HOAS.num(3))), 5));
test('lam + app: id', () => assert.equal(evalHOAS(HOAS.app(HOAS.lam(x => x), HOAS.num(42))), 42));
test('lam + app: inc', () => assert.equal(evalHOAS(HOAS.app(HOAS.lam(x => HOAS.add(x, HOAS.num(1))), HOAS.num(41))), 42));
test('K combinator', () => assert.equal(evalHOAS(HOAS.app(HOAS.app(HOAS.lam(x => HOAS.lam(y => x)), HOAS.num(1)), HOAS.num(2))), 1));
test('let', () => assert.equal(evalHOAS(HOAS.let_(HOAS.num(5), x => HOAS.add(x, HOAS.num(1)))), 6));
test('size: num = 1', () => assert.equal(sizeHOAS(HOAS.num(42)), 1));
test('size: add = 3', () => assert.equal(sizeHOAS(HOAS.add(HOAS.num(1), HOAS.num(2))), 3));
test('size: lam = 2', () => assert.equal(sizeHOAS(HOAS.lam(x => x)), 2));
test('nested: (2+3)*(4+5)', () => assert.equal(evalHOAS(HOAS.app(HOAS.lam(x => HOAS.add(x, x)), HOAS.num(21))), 42));

console.log(`\nHOAS tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
