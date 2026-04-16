import { strict as assert } from 'assert';
import { Var, Num, Lam, App, Add, eval_, obsEqual, structEqual, groupEquivalent } from './program-equiv.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const id = new Lam('x', new Var('x'));
const add1 = new Lam('x', new Add(new Var('x'), new Num(1)));
const add1b = new Lam('y', new Add(new Var('y'), new Num(1)));
const add2 = new Lam('x', new Add(new Var('x'), new Num(2)));

test('eval: (λx.x) 42 = 42', () => assert.equal(eval_(new App(id, new Num(42))), 42));
test('eval: 2 + 3 = 5', () => assert.equal(eval_(new Add(new Num(2), new Num(3))), 5));

test('obsEqual: id = id', () => assert.ok(obsEqual(id, id).equal));
test('obsEqual: λx.x+1 = λy.y+1', () => assert.ok(obsEqual(add1, add1b).equal));
test('obsEqual: λx.x+1 ≠ λx.x+2', () => assert.ok(!obsEqual(add1, add2).equal));
test('obsEqual: witness provided', () => {
  const r = obsEqual(add1, add2);
  assert.ok(r.witness !== undefined);
});

test('structEqual: same = true', () => assert.ok(structEqual(id, id)));
test('structEqual: alpha = true', () => assert.ok(structEqual(add1, add1b)));
test('structEqual: different = false', () => assert.ok(!structEqual(add1, add2)));

test('groupEquivalent: groups equal terms', () => {
  const groups = groupEquivalent([add1, add1b, add2, id]);
  assert.equal(groups.length, 3); // {add1,add1b}, {add2}, {id}
});

console.log(`\nProgram equivalence tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
