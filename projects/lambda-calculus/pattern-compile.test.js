import { strict as assert } from 'assert';
import {
  PWild, PVar, PCon, PLit, Clause,
  DLeaf, DSwitch, DFail,
  VCon, VLit,
  PatternCompiler, evalDecisionTree
} from './pattern-compile.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Simple patterns
// ============================================================

test('wildcard: always matches', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [new Clause([new PWild()], 'body1')]);
  assert.equal(tree.tag, 'DLeaf');
});

test('variable: binds name', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [new Clause([new PVar('n')], 'body1')]);
  assert.equal(tree.tag, 'DLeaf');
  assert.ok(tree.bindings.some(b => b.name === 'n'));
});

test('literal: creates switch', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [
    new Clause([new PLit(1)], 'one'),
    new Clause([new PLit(2)], 'two'),
    new Clause([new PWild()], 'other'),
  ]);
  assert.equal(tree.tag, 'DSwitch');
});

// ============================================================
// Constructor patterns
// ============================================================

test('constructor: Just/Nothing', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [
    new Clause([new PCon('Just', [new PVar('v')])], 'found'),
    new Clause([new PCon('Nothing', [])], 'empty'),
  ]);
  assert.equal(tree.tag, 'DSwitch');
  assert.ok(tree.cases.has('Just'));
  assert.ok(tree.cases.has('Nothing'));
});

test('constructor eval: Just(42) matches Just(v)', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [
    new Clause([new PCon('Just', [new PVar('v')])], 'found'),
    new Clause([new PCon('Nothing', [])], 'empty'),
  ]);
  const env = new Map([['x', new VCon('Just', [new VLit(42)])]]);
  const result = evalDecisionTree(tree, env);
  assert.ok(result.matched);
  assert.equal(result.body, 'found');
});

test('constructor eval: Nothing matches Nothing', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [
    new Clause([new PCon('Just', [new PVar('v')])], 'found'),
    new Clause([new PCon('Nothing', [])], 'empty'),
  ]);
  const env = new Map([['x', new VCon('Nothing', [])]]);
  const result = evalDecisionTree(tree, env);
  assert.ok(result.matched);
  assert.equal(result.body, 'empty');
});

// ============================================================
// Literal patterns
// ============================================================

test('literal eval: 1 matches PLit(1)', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [
    new Clause([new PLit(1)], 'one'),
    new Clause([new PLit(2)], 'two'),
    new Clause([new PWild()], 'other'),
  ]);
  const env = new Map([['x', new VLit(1)]]);
  const result = evalDecisionTree(tree, env);
  assert.ok(result.matched);
  assert.equal(result.body, 'one');
});

test('literal eval: 3 matches wildcard', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [
    new Clause([new PLit(1)], 'one'),
    new Clause([new PLit(2)], 'two'),
    new Clause([new PWild()], 'other'),
  ]);
  const env = new Map([['x', new VLit(3)]]);
  const result = evalDecisionTree(tree, env);
  assert.ok(result.matched);
  assert.equal(result.body, 'other');
});

// ============================================================
// Multiple columns
// ============================================================

test('two columns: pair matching', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x', 'y'], [
    new Clause([new PLit(0), new PWild()], 'x-zero'),
    new Clause([new PWild(), new PLit(0)], 'y-zero'),
    new Clause([new PWild(), new PWild()], 'both-nonzero'),
  ]);
  assert.equal(tree.tag, 'DSwitch');
});

// ============================================================
// Exhaustiveness
// ============================================================

test('no clauses: DFail', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], []);
  assert.equal(tree.tag, 'DFail');
});

test('non-exhaustive: unmatched case → DFail', () => {
  const compiler = new PatternCompiler();
  const tree = compiler.compile(['x'], [
    new Clause([new PLit(1)], 'one'),
  ]);
  const env = new Map([['x', new VLit(2)]]);
  const result = evalDecisionTree(tree, env);
  assert.ok(!result.matched);
});

// ============================================================
// Report
// ============================================================

console.log(`\nPattern matching compilation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
