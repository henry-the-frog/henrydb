import { strict as assert } from 'assert';
import { Num, Var, Add, Mul, If0, Let, partialEval, specialize, countOps } from './partial-eval.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('constant folding: 2 + 3 → 5', () => assert.equal(partialEval(new Add(new Num(2), new Num(3))).n, 5));
test('constant folding: 2 * 3 → 6', () => assert.equal(partialEval(new Mul(new Num(2), new Num(3))).n, 6));
test('add identity: 0 + x → x', () => assert.equal(partialEval(new Add(new Num(0), new Var('x'))).name, 'x'));
test('mul identity: 1 * x → x', () => assert.equal(partialEval(new Mul(new Num(1), new Var('x'))).name, 'x'));
test('mul zero: 0 * x → 0', () => assert.equal(partialEval(new Mul(new Num(0), new Var('x'))).n, 0));

test('if0 static: if0 0 then 1 else 2 → 1', () => {
  assert.equal(partialEval(new If0(new Num(0), new Num(1), new Num(2))).n, 1);
});

test('if0 dynamic: preserved', () => {
  const r = partialEval(new If0(new Var('x'), new Num(1), new Num(2)));
  assert.equal(r.tag, 'If0');
});

test('let: inline', () => {
  const r = partialEval(new Let('x', new Num(5), new Add(new Var('x'), new Num(1))));
  assert.equal(r.n, 6);
});

test('specialize: f(x,y) with x=3', () => {
  const expr = new Add(new Var('x'), new Var('y')); // x + y
  const r = specialize(expr, { x: 3 });
  assert.equal(r.tag, 'Add');
  assert.equal(r.left.n, 3); // x is now 3
  assert.equal(r.right.name, 'y'); // y stays dynamic
});

test('specialize: fully known → constant', () => {
  const expr = new Mul(new Var('x'), new Add(new Var('y'), new Num(1)));
  const r = specialize(expr, { x: 4, y: 9 });
  assert.equal(r.n, 40);
});

test('countOps: reduced is smaller', () => {
  const expr = new Add(new Num(2), new Add(new Num(3), new Var('x')));
  assert.ok(countOps(partialEval(expr)) <= countOps(expr));
});

console.log(`\nPartial evaluation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
