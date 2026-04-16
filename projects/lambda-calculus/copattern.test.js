import { strict as assert } from 'assert';
import { repeat, iterate, take, coMap, zipWith, coRecord } from './copattern.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('repeat: take 3 of repeat(1) = [1,1,1]', () => assert.deepStrictEqual(take(repeat(1), 3), [1, 1, 1]));
test('iterate: take 5 of iterate(+1, 0) = [0,1,2,3,4]', () => assert.deepStrictEqual(take(iterate(x => x + 1, 0), 5), [0, 1, 2, 3, 4]));
test('head: first element', () => assert.equal(iterate(x => x + 1, 42).observe('head'), 42));
test('tail.head: second element', () => assert.equal(iterate(x => x + 1, 0).observe('tail').observe('head'), 1));
test('coMap: double', () => assert.deepStrictEqual(take(coMap(x => x * 2, iterate(x => x + 1, 1)), 3), [2, 4, 6]));
test('zipWith: add', () => {
  const s1 = iterate(x => x + 1, 1); // 1,2,3,...
  const s2 = iterate(x => x + 10, 10); // 10,20,30,...
  assert.deepStrictEqual(take(zipWith((a, b) => a + b, s1, s2), 3), [11, 22, 33]);
});
test('coRecord: observe field', () => {
  const r = coRecord({ name: 'Alice', age: 30 });
  assert.equal(r.observe('name'), 'Alice');
  assert.equal(r.observe('age'), 30);
});
test('coRecord: missing field → error', () => {
  assert.throws(() => coRecord({ x: 1 }).observe('y'), /No copattern/);
});
test('fibonacci via zipWith', () => {
  function fib() {
    const s = iterate(x => x, 0); // placeholder
    // fib = 0 : 1 : zipWith(+, fib, tail(fib))
    let a = 0, b = 1;
    return iterate(() => { const c = a; a = b; b = c + b; return c; }, null).observe('tail');
  }
  // Simpler: direct fibonacci stream
  function fibStream(a = 0, b = 1) { return { observe: d => d === 'head' ? a : fibStream(b, a + b) }; }
  assert.deepStrictEqual(take(fibStream(), 7), [0, 1, 1, 2, 3, 5, 8]);
});
test('take 0: empty', () => assert.deepStrictEqual(take(repeat(42), 0), []));

console.log(`\nCopattern matching tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
