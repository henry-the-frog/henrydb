import { strict as assert } from 'assert';
import { canonicalForm, verifyCanonicalForms } from './canonical-forms.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Int: integer', () => assert.ok(canonicalForm(42, 'Int').canonical));
test('Int: not float', () => assert.ok(!canonicalForm(3.14, 'Int').canonical));
test('Bool: true', () => assert.equal(canonicalForm(true, 'Bool').form, 'boolean'));
test('Bool: not int', () => assert.ok(!canonicalForm(1, 'Bool').canonical));
test('String: canonical', () => assert.ok(canonicalForm('hello', 'String').canonical));
test('Unit: null', () => assert.ok(canonicalForm(null, 'Unit').canonical));
test('Fun: lambda', () => assert.equal(canonicalForm(x => x, 'Fun(Int,Int)').form, 'lambda'));
test('List: nil', () => assert.equal(canonicalForm([], 'List(Int)').form, 'nil'));
test('List: cons', () => assert.equal(canonicalForm([1, 2], 'List(Int)').form, 'cons'));
test('Pair: pair', () => assert.equal(canonicalForm([1, 'a'], 'Pair(Int,String)').form, 'pair'));
test('verify batch', () => {
  const results = verifyCanonicalForms([
    { value: 42, type: 'Int' }, { value: true, type: 'Bool' }, { value: 'hi', type: 'String' }
  ]);
  assert.ok(results.every(r => r.canonical));
});

console.log(`\n🎉🎉🎉 MODULE #190!!! Canonical forms tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
