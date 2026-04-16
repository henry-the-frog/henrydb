import { strict as assert } from 'assert';
import { SNum, SAdd, SMul, evalSource, compile, evalTarget, verifyCorrespondence, randomExpr, bisimulation } from './operational-correspondence.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('source: 2+3', () => assert.equal(evalSource(new SAdd(new SNum(2), new SNum(3))), 5));
test('source: 4*5', () => assert.equal(evalSource(new SMul(new SNum(4), new SNum(5))), 20));

test('compile: num → Push', () => {
  const instrs = compile(new SNum(42));
  assert.equal(instrs.length, 1);
  assert.equal(instrs[0].n, 42);
});

test('target: Push 2, Push 3, Add → 5', () => {
  assert.equal(evalTarget(compile(new SAdd(new SNum(2), new SNum(3)))), 5);
});

test('correspondence: 2+3', () => {
  assert.ok(verifyCorrespondence(new SAdd(new SNum(2), new SNum(3))).correct);
});

test('correspondence: (2+3)*4', () => {
  assert.ok(verifyCorrespondence(new SMul(new SAdd(new SNum(2), new SNum(3)), new SNum(4))).correct);
});

test('correspondence: nested', () => {
  const e = new SAdd(new SMul(new SNum(2), new SNum(3)), new SMul(new SNum(4), new SNum(5)));
  assert.ok(verifyCorrespondence(e).correct);
});

test('correspondence: random (property test)', () => {
  for (let i = 0; i < 20; i++) {
    const e = randomExpr(3);
    assert.ok(verifyCorrespondence(e).correct, `Failed on random expr`);
  }
});

test('bisimulation: same steps', () => {
  assert.ok(bisimulation([1, 2, 3], [1, 2, 3]).bisimilar);
});

test('bisimulation: different steps', () => {
  assert.ok(!bisimulation([1, 2], [1, 3]).bisimilar);
});

console.log(`\nOperational correspondence tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
