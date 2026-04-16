import { strict as assert } from 'assert';
import { DVar, DCon, DWild, matchPattern, isExhaustive, findRedundant } from './dep-pattern-match.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('match: variable', () => {
  const r = matchPattern(new DVar('x'), new DCon('Succ', [new DCon('Zero', [])]));
  assert.ok(r);
  assert.ok(r.bindings.has('x'));
});

test('match: wildcard', () => {
  const r = matchPattern(new DWild(), new DCon('Zero', []));
  assert.ok(r);
  assert.equal(r.bindings.size, 0);
});

test('match: constructor', () => {
  const r = matchPattern(new DCon('Zero', []), new DCon('Zero', []));
  assert.ok(r);
});

test('match: constructor mismatch', () => {
  const r = matchPattern(new DCon('Zero', []), new DCon('Succ', [new DCon('Zero', [])]));
  assert.equal(r, null);
});

test('match: nested', () => {
  const r = matchPattern(
    new DCon('Succ', [new DVar('n')]),
    new DCon('Succ', [new DCon('Zero', [])])
  );
  assert.ok(r);
  assert.ok(r.bindings.has('n'));
});

test('match: type refinement', () => {
  const r = matchPattern(new DCon('Zero', []), new DCon('Zero', []), { 'Zero': { n: 'Z' } });
  assert.ok(r.refinements.has('n'));
  assert.equal(r.refinements.get('n'), 'Z');
});

test('exhaustive: all constructors covered', () => {
  assert.ok(isExhaustive([new DCon('Zero', []), new DCon('Succ', [new DWild()])], ['Zero', 'Succ']));
});

test('exhaustive: missing constructor', () => {
  assert.ok(!isExhaustive([new DCon('Zero', [])], ['Zero', 'Succ']));
});

test('exhaustive: wildcard covers all', () => {
  assert.ok(isExhaustive([new DWild()], ['Zero', 'Succ']));
});

test('redundant: duplicate patterns', () => {
  assert.deepStrictEqual(findRedundant([new DCon('Zero', []), new DCon('Zero', [])]), [1]);
});

test('redundant: no duplicates', () => {
  assert.deepStrictEqual(findRedundant([new DCon('Zero', []), new DCon('Succ', [new DWild()])]), []);
});

console.log(`\nDependent pattern matching tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
