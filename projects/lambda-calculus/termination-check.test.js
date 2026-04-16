import { strict as assert } from 'assert';
import { CallGraph, analyzeRecursion, isWellFounded } from './termination-check.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('CallGraph: decreasing → terminates', () => {
  const cg = new CallGraph();
  cg.addCall('f', 'f', [{ name: 'n-1', smaller: true, than: 'n' }]);
  assert.ok(cg.checkTermination('f', ['n']).terminates);
});

test('CallGraph: non-decreasing → fails', () => {
  const cg = new CallGraph();
  cg.addCall('f', 'f', [{ name: 'n', smaller: false, than: null }]);
  assert.ok(!cg.checkTermination('f', ['n']).terminates);
});

test('CallGraph: multiple params, one decreases → ok', () => {
  const cg = new CallGraph();
  cg.addCall('f', 'f', [
    { name: 'x', smaller: false, than: null },
    { name: 'n-1', smaller: true, than: 'm' }
  ]);
  assert.ok(cg.checkTermination('f', ['x', 'm']).terminates);
});

test('analyzeRecursion: match with binding → terminates', () => {
  const fn = {
    name: 'length', params: ['xs'],
    body: { tag: 'Match', scrutinee: 'xs', branches: [
      { pattern: 'Nil', body: { tag: 'Num', value: 0 } },
      { pattern: 'Cons', bindings: [{ name: 'tail' }],
        body: { tag: 'Call', fn: 'length', args: [{ name: 'tail' }] } }
    ]}
  };
  assert.ok(analyzeRecursion(fn).terminates);
});

test('isWellFounded: strict order', () => {
  assert.ok(isWellFounded([[3,2], [2,1], [1,0]], [0,1,2,3]));
});

test('isWellFounded: reflexive → false', () => {
  assert.ok(!isWellFounded([[1,1]], [1]));
});

test('isWellFounded: cycle → false', () => {
  assert.ok(!isWellFounded([[1,2], [2,1]], [1,2]));
});

test('isWellFounded: empty → true', () => {
  assert.ok(isWellFounded([], [1,2,3]));
});

test('CallGraph: no recursive calls → terminates', () => {
  const cg = new CallGraph();
  assert.ok(cg.checkTermination('f', ['n']).terminates);
});

test('CallGraph: mutual recursion detection', () => {
  const cg = new CallGraph();
  cg.addCall('f', 'g', [{ name: 'n', smaller: false }]);
  cg.addCall('g', 'f', [{ name: 'n-1', smaller: true, than: 'n' }]);
  // Only self-calls matter for checkTermination
  assert.ok(cg.checkTermination('f', ['n']).terminates);
});

console.log(`\nTermination checking tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
