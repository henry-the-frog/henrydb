import { strict as assert } from 'assert';
import { Pattern, checkCoverage, CallGraph, checkProductivity, checkMetric } from './totality.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

const boolType = { constructors: [{ name: 'True' }, { name: 'False' }] };
const listType = { constructors: [{ name: 'Nil' }, { name: 'Cons' }] };
const maybeType = { constructors: [{ name: 'Nothing' }, { name: 'Just' }] };

// Coverage
test('coverage: all cases → exhaustive', () => {
  const r = checkCoverage(boolType, [Pattern.con('True'), Pattern.con('False')]);
  assert.ok(r.exhaustive);
});

test('coverage: missing case', () => {
  const r = checkCoverage(boolType, [Pattern.con('True')]);
  assert.ok(!r.exhaustive);
  assert.deepStrictEqual(r.missing, ['False']);
});

test('coverage: wildcard covers all', () => {
  const r = checkCoverage(listType, [Pattern.wildcard()]);
  assert.ok(r.exhaustive);
});

test('coverage: variable covers all', () => {
  const r = checkCoverage(maybeType, [Pattern.var('x')]);
  assert.ok(r.exhaustive);
});

test('coverage: partial list', () => {
  const r = checkCoverage(listType, [Pattern.con('Nil')]);
  assert.ok(!r.exhaustive);
  assert.deepStrictEqual(r.missing, ['Cons']);
});

// Termination
test('termination: no recursion → terminates', () => {
  const cg = new CallGraph();
  cg.addCall('f', 'g', []); // Not recursive
  assert.ok(cg.checkTermination('f').terminates);
});

test('termination: structural decrease', () => {
  const cg = new CallGraph();
  cg.addCall('length', 'length', [{ decreasing: true }]);
  assert.ok(cg.checkTermination('length').terminates);
});

test('termination: no decrease → fails', () => {
  const cg = new CallGraph();
  cg.addCall('loop', 'loop', [{ decreasing: false }]);
  assert.ok(!cg.checkTermination('loop').terminates);
});

// Productivity
test('productivity: guarded → productive', () => {
  const r = checkProductivity({ guardedBy: 'Cons' });
  assert.ok(r.productive);
});

test('productivity: unguarded → not productive', () => {
  const r = checkProductivity({});
  assert.ok(!r.productive);
});

// Metric
test('metric: all non-negative', () => {
  const r = checkMetric(null, [5, 3, 1, 0], x => x);
  assert.ok(r.allValid);
});

test('metric: negative → invalid', () => {
  const r = checkMetric(null, [5, -1, 3], x => x);
  assert.ok(!r.allValid);
});

console.log(`\nTotality checking tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
