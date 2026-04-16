import { strict as assert } from 'assert';
import { Lens, Prism, Iso, composeLens, prop, index, tagged, celsiusFahrenheit, stringNumber } from './profunctor-optics.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('prop lens: view', () => assert.equal(prop('name').view({ name: 'Alice', age: 30 }), 'Alice'));
test('prop lens: over', () => {
  const r = prop('age').over(x => x + 1, { name: 'Alice', age: 30 });
  assert.equal(r.age, 31);
  assert.equal(r.name, 'Alice');
});

test('index lens: view', () => assert.equal(index(1).view([10, 20, 30]), 20));
test('index lens: over', () => assert.deepStrictEqual(index(0).over(x => x * 2, [5, 10]), [10, 10]));

test('compose: nested', () => {
  const nameLen = composeLens(prop('name'), new Lens(s => s.length, (n, s) => s));
  assert.equal(nameLen.view({ name: 'Alice' }), 5);
});

test('prism: match success', () => assert.equal(tagged('Some').preview({ tag: 'Some', value: 42 }), 42));
test('prism: match failure', () => assert.equal(tagged('Some').preview({ tag: 'None', value: null }), null));
test('prism: review', () => assert.deepStrictEqual(tagged('Some').review(42), { tag: 'Some', value: 42 }));

test('iso: celsius → fahrenheit', () => assert.equal(celsiusFahrenheit.view(100), 212));
test('iso: fahrenheit → celsius', () => assert.equal(celsiusFahrenheit.review(32), 0));
test('iso roundtrip', () => assert.equal(celsiusFahrenheit.review(celsiusFahrenheit.view(37)), 37));
test('stringNumber: roundtrip', () => assert.equal(stringNumber.review(stringNumber.view('42')), '42'));

console.log(`\nProfunctor optics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
