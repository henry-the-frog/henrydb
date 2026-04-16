import { strict as assert } from 'assert';
import { evalInterp, prettyInterp, sizeInterp, example1, example2, example3, example4, resetPP } from './tagless-final.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// Eval interpreter
test('eval: 2+3=5', () => assert.equal(example1(evalInterp()), 5));
test('eval: (1+2)*4=12', () => assert.equal(example2(evalInterp()), 12));
test('eval: if true then 1 else 2 = 1', () => assert.equal(example3(evalInterp()), 1));
test('eval: (λx.x+1) 41 = 42', () => assert.equal(example4(evalInterp()), 42));

// Pretty printer
test('pretty: 2+3', () => {
  resetPP();
  assert.equal(example1(prettyInterp()), '(2 + 3)');
});
test('pretty: (1+2)*4', () => {
  resetPP();
  assert.equal(example2(prettyInterp()), '((1 + 2) * 4)');
});

// Size interpreter
test('size: 2+3 = 3 nodes', () => assert.equal(example1(sizeInterp()), 3));
test('size: (1+2)*4 = 5 nodes', () => assert.equal(example2(sizeInterp()), 5));

// Same program, three interpretations
test('same program, 3 interpretations', () => {
  resetPP();
  const ev = example2(evalInterp());
  const pp = example2(prettyInterp());
  const sz = example2(sizeInterp());
  assert.equal(ev, 12);
  assert.ok(pp.includes('+'));
  assert.ok(sz > 0);
});

test('eval: negation', () => {
  const i = evalInterp();
  assert.equal(i.neg(i.num(5)), -5);
});

console.log(`\nTagless final tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
