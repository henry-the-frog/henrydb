import { strict as assert } from 'assert';
import { BOT, FlatDomain, LiftedDomain, ProductDomain, fix, kleeneChain, factDenotation } from './domain-theory.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('flat domain: ⊥ ⊑ x', () => {
  const d = new FlatDomain('Int', new Set([1, 2, 3]));
  assert.ok(d.leq(BOT, 1));
  assert.ok(d.leq(BOT, 2));
});

test('flat domain: x ⊑ x', () => {
  const d = new FlatDomain('Int', new Set([1]));
  assert.ok(d.leq(1, 1));
});

test('flat domain: x ⋢ y for x ≠ y', () => {
  const d = new FlatDomain('Int', new Set([1, 2]));
  assert.ok(!d.leq(1, 2));
});

test('lifted domain: lub(⊥, x) = x', () => {
  const d = new LiftedDomain([1, 2, 3]);
  assert.equal(d.lub(BOT, 42), 42);
});

test('lifted domain: lub(x, x) = x', () => {
  const d = new LiftedDomain([1]);
  assert.equal(d.lub(5, 5), 5);
});

test('product domain: componentwise order', () => {
  const d = new ProductDomain(new LiftedDomain([]), new LiftedDomain([]));
  assert.ok(d.leq([BOT, BOT], [1, 2]));
  assert.ok(d.leq([1, BOT], [1, 2]));
});

test('fix: factorial via fixed point', () => {
  const fact = factDenotation();
  assert.equal(fact(0), 1);
  assert.equal(fact(5), 120);
  assert.equal(fact(10), 3628800);
});

test('fix: simple iteration', () => {
  // fix(f)(x) = f(x) when f has a fixed point
  const result = fix(x => Math.cos(x), 0, 1000, (a, b) => Math.abs(a - b) < 1e-10);
  assert.ok(Math.abs(result - Math.cos(result)) < 1e-9);
});

test('kleene chain: f(x) = x/2 from 100', () => {
  const chain = kleeneChain(x => Math.floor(x / 2), 100, 10);
  assert.equal(chain[0], 100);
  assert.equal(chain[1], 50);
  assert.equal(chain[2], 25);
});

test('bottom is identity for lub', () => {
  const d = new LiftedDomain([]);
  assert.equal(d.lub(BOT, 42), 42);
  assert.equal(d.lub(42, BOT), 42);
});

console.log(`\nDomain theory tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
