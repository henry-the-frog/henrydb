import { strict as assert } from 'assert';
import { Void, Unit, Bool, Byte, Sum, Prod, Exp, algebraicIdentities, derivative, evalTypeExpr } from './type-algebra.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// All algebraic identities should hold
const identities = algebraicIdentities();
for (const id of identities) {
  test(`identity: ${id.name}`, () => assert.ok(id.holds, `${id.left} ≠ ${id.right}`));
}

// Additional counts
test('Maybe Bool = 3 inhabitants', () => assert.equal(Sum(Unit(), Bool()), 3));
test('Either Bool Unit = 3', () => assert.equal(Sum(Bool(), Unit()), 3));
test('(Bool, Bool) = 4', () => assert.equal(Prod(Bool(), Bool()), 4));
test('Bool → Bool = 4 functions', () => assert.equal(Exp(Bool(), Bool()), 4));
test('Unit → A = A', () => assert.equal(Exp(Unit(), 42), 42));
test('Void → A = 1', () => assert.equal(Exp(Void(), 42), 1));

// Derivative
test('derivative: d/da(a×a) = a + a', () => {
  const aSquared = { tag: 'TProd', left: { tag: 'TVar', value: 3 }, right: { tag: 'TVar', value: 3 } };
  const d = derivative(aSquared);
  assert.equal(evalTypeExpr(d), 6); // 1×3 + 3×1 = 6
});

console.log(`\nType algebra tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
