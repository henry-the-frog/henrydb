import { strict as assert } from 'assert';
import { EXAMPLES, countTypeVars, countArrows, isContradiction, verifyInhabitant, typeComplexity } from './type-tetris.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('id: A → A', () => assert.equal(EXAMPLES[0].impl(42), 42));
test('const: A → B → A', () => assert.equal(EXAMPLES[1].impl(1)(2), 1));
test('const\': A → B → B', () => assert.equal(EXAMPLES[2].impl(1)(2), 2));
test('apply: (A→B)→A→B', () => assert.equal(EXAMPLES[3].impl(x => x + 1)(41), 42));
test('flip: (A→B→C)→B→A→C', () => assert.equal(EXAMPLES[4].impl(a => b => a - b)(10)(3), -7));
test('compose: (B→C)→(A→B)→A→C', () => assert.equal(EXAMPLES[5].impl(x => x + 1)(x => x * 2)(3), 7));
test('dup: (A→A→B)→A→B', () => assert.equal(EXAMPLES[6].impl(x => y => x + y)(5), 10));
test('peirce: not inhabited', () => assert.equal(EXAMPLES[7].impl, null));
test('A→Void: not inhabited', () => assert.equal(EXAMPLES[8].impl, null));

test('countTypeVars: A→B→C = 3', () => assert.equal(countTypeVars('A → B → C'), 3));
test('countArrows: A→B→C = 2', () => assert.equal(countArrows('A → B → C'), 2));
test('typeComplexity: measures', () => assert.ok(typeComplexity('(A → B) → A → B').arrows === 3));

console.log(`\nType tetris tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
