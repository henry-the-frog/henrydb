import { strict as assert } from 'assert';
import { provable, Atom, Imp, And, Or } from './proof-search.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const A = Atom('A'), B = Atom('B'), C = Atom('C');

test('A → A', () => assert.ok(provable(Imp(A, A)).proved));
test('A → B → A', () => assert.ok(provable(Imp(A, Imp(B, A))).proved));
test('A → B → B', () => assert.ok(provable(Imp(A, Imp(B, B))).proved));
test('A → A ∧ A', () => assert.ok(provable(Imp(A, And(A, A))).proved));
test('A → A ∨ B', () => assert.ok(provable(Imp(A, Or(A, B))).proved));
test('A ∧ B → A', () => assert.ok(provable(Imp(And(A, B), A)).proved));
test('A ∧ B → B', () => assert.ok(provable(Imp(And(A, B), B)).proved));
test('A → B (unprovable)', () => assert.ok(!provable(Imp(A, B)).proved));
test('A (no hyps, unprovable)', () => assert.ok(!provable(A).proved));
test('A with A in hyps', () => assert.ok(provable(A, [A]).proved));

console.log(`\nProof search tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
