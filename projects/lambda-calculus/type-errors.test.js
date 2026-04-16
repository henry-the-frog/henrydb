import { strict as assert } from 'assert';
import {
  Span, Diagnostic, typeMismatch, unboundVariable,
  arityMismatch, infiniteType, missingField, unusedVariable,
  levenshtein, findSimilar
} from './type-errors.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('typeMismatch: formats correctly', () => {
  const d = typeMismatch('Int', 'String', new Span(1, 5));
  const fmt = d.format();
  assert.ok(fmt.includes('Type mismatch'));
  assert.ok(fmt.includes('1:5'));
});

test('typeMismatch: Int/String suggests parseInt', () => {
  const d = typeMismatch('Int', 'String');
  assert.ok(d.suggestions.length > 0);
  assert.ok(d.suggestions[0].message.includes('parseInt'));
});

test('unboundVariable: with suggestions', () => {
  const d = unboundVariable('tset', null, ['test', 'set']);
  const fmt = d.format();
  assert.ok(fmt.includes('test'));
});

test('arityMismatch: formatted', () => {
  const d = arityMismatch('foo', 2, 3);
  assert.ok(d.format().includes('2'));
});

test('infiniteType: has note', () => {
  const d = infiniteType('a', 'a → Int');
  assert.ok(d.notes.length > 0);
});

test('unusedVariable: is warning', () => {
  const d = unusedVariable('x');
  assert.equal(d.severity, 'warning');
  assert.ok(d.suggestions[0].message.includes('_x'));
});

test('missingField', () => {
  const d = missingField('{x: Int}', 'y');
  assert.ok(d.format().includes("'y'"));
});

// Levenshtein
test('levenshtein: same = 0', () => assert.equal(levenshtein('test', 'test'), 0));
test('levenshtein: one edit', () => assert.equal(levenshtein('test', 'tast'), 1));
test('levenshtein: different', () => assert.ok(levenshtein('abc', 'xyz') > 2));

// Find similar
test('findSimilar: suggests close matches', () => {
  const similar = findSimilar('tset', ['test', 'best', 'rest', 'xyz']);
  assert.ok(similar.includes('test'));
  assert.ok(!similar.includes('xyz'));
});

test('diagnostic: chain notes and suggestions', () => {
  const d = new Diagnostic('error', 'Something broke')
    .addNote('Here is why')
    .addSuggestion('Try this');
  assert.equal(d.notes.length, 1);
  assert.equal(d.suggestions.length, 1);
});

console.log(`\nType error messages tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
