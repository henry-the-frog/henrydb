import { strict as assert } from 'assert';
import { Z, factorial, fibonacci, gcd, ackermann, isEven, isOdd, memoFib } from './fixed-point.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Z: identity', () => { const id = Z(f => x => x); assert.equal(id(42), 42); });
test('factorial: 0! = 1', () => assert.equal(factorial(0), 1));
test('factorial: 5! = 120', () => assert.equal(factorial(5), 120));
test('fibonacci: fib(0) = 0', () => assert.equal(fibonacci(0), 0));
test('fibonacci: fib(10) = 55', () => assert.equal(fibonacci(10), 55));
test('gcd: gcd(12, 8) = 4', () => assert.equal(gcd(12, 8), 4));
test('ackermann: A(0,0) = 1', () => assert.equal(ackermann(0, 0), 1));
test('ackermann: A(1,1) = 3', () => assert.equal(ackermann(1, 1), 3));
test('ackermann: A(2,2) = 7', () => assert.equal(ackermann(2, 2), 7));

test('mutual: isEven(4)', () => assert.ok(isEven(4)));
test('mutual: isOdd(3)', () => assert.ok(isOdd(3)));
test('mutual: !isEven(3)', () => assert.ok(!isEven(3)));

test('memoFib: fast for large n', () => assert.equal(memoFib(30), 832040));

console.log(`\nFixed-point combinator tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
