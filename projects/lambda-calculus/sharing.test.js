import { strict as assert } from 'assert';
import { Var, Lam, App, Let, Num, usageCounts, classify, deadVars, inlineUnique, exprSize } from './sharing.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('usageCounts: x used twice', () => {
  const expr = new App(new Var('x'), new Var('x'));
  assert.equal(usageCounts(expr).get('x'), 2);
});

test('classify: dead', () => assert.equal(classify('x', new Map()), 'dead'));
test('classify: unique', () => assert.equal(classify('x', new Map([['x', 1]])), 'unique'));
test('classify: shared', () => assert.equal(classify('x', new Map([['x', 3]])), 'shared'));

test('deadVars: unused let binding', () => {
  const expr = new Let('x', new Num(42), new Num(1));
  assert.ok(deadVars(expr).includes('x'));
});

test('inlineUnique: let x = 5 in x → 5', () => {
  const expr = new Let('x', new Num(5), new Var('x'));
  const r = inlineUnique(expr);
  assert.equal(r.n, 5);
});

test('inlineUnique: let x = 5 in x + x → preserved (shared)', () => {
  const expr = new Let('x', new Num(5), new App(new Var('x'), new Var('x')));
  const r = inlineUnique(expr);
  assert.equal(r.tag, 'Let'); // Not inlined because used twice
});

test('inlineUnique: dead let removed', () => {
  const expr = new Let('x', new Num(42), new Num(1));
  const r = inlineUnique(expr);
  assert.equal(r.n, 1); // Dead binding removed
});

test('exprSize: variable = 1', () => assert.equal(exprSize(new Var('x')), 1));
test('exprSize: app = 1 + children', () => assert.equal(exprSize(new App(new Var('f'), new Var('x'))), 3));

test('usageCounts: under lambda', () => {
  const expr = new Lam('x', new App(new Var('x'), new Var('y')));
  const counts = usageCounts(expr);
  assert.equal(counts.get('x'), 1);
  assert.equal(counts.get('y'), 1);
});

console.log(`\nSharing analysis tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
