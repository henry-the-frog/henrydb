import { strict as assert } from 'assert';
import {
  RELEVANT, IRRELEVANT,
  EVar, ELam, EApp, ELet, ENum, EType,
  erase, nodeCount, erasureStats, Newtype
} from './proof-irrelevance.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('erase: relevant variable preserved', () => {
  const e = erase(new EVar('x'));
  assert.equal(e.tag, 'EVar');
});

test('erase: irrelevant variable → unit', () => {
  const e = erase(new EVar('proof', IRRELEVANT));
  assert.equal(e.tag, 'EUnit');
});

test('erase: type term → unit', () => {
  const e = erase(new EType('Int'));
  assert.equal(e.tag, 'EUnit');
});

test('erase: irrelevant lambda removed', () => {
  const expr = new ELam('T', IRRELEVANT, new ENum(42));
  const e = erase(expr);
  assert.equal(e.tag, 'ENum');
  assert.equal(e.n, 42);
});

test('erase: relevant lambda preserved', () => {
  const expr = new ELam('x', RELEVANT, new EVar('x'));
  const e = erase(expr);
  assert.equal(e.tag, 'ELam');
});

test('erase: irrelevant application removed', () => {
  const expr = new EApp(new ELam('x', RELEVANT, new EVar('x')), new EType('Int'), IRRELEVANT);
  const e = erase(expr);
  assert.equal(e.tag, 'ELam'); // App erased, only fn remains
});

test('erase: irrelevant let removed', () => {
  const expr = new ELet('proof', IRRELEVANT, new EType('P'), new ENum(42));
  const e = erase(expr);
  assert.equal(e.tag, 'ENum');
});

// Size analysis
test('erasureStats: mixed expr', () => {
  const expr = new EApp(
    new ELam('T', IRRELEVANT, new ELam('x', RELEVANT, new EVar('x'))),
    new EType('Int'),
    IRRELEVANT
  );
  const stats = erasureStats(expr);
  assert.ok(stats.erased > 0);
  assert.ok(stats.after < stats.before);
});

test('nodeCount: simple', () => {
  assert.equal(nodeCount(new ENum(42)), 1);
  assert.equal(nodeCount(new ELam('x', RELEVANT, new EVar('x'))), 2);
});

// Newtype coercions
test('newtype: zero-cost wrap/unwrap', () => {
  const Age = new Newtype('Age', 'Int');
  const wrapped = Age.wrap(25);
  const unwrapped = Age.unwrap(wrapped);
  assert.equal(unwrapped, 25);
  assert.equal(wrapped, 25); // Same representation!
});

test('erase: complex dependent expression', () => {
  // Simulating: Λ(A:*). λ(x:A). x  →  λx. x (after erasure)
  const expr = new ELam('A', IRRELEVANT,
    new ELam('x', RELEVANT, new EVar('x')));
  const e = erase(expr);
  assert.equal(e.tag, 'ELam');
  assert.equal(e.var, 'x');
});

console.log(`\nProof irrelevance tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
