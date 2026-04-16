import { strict as assert } from 'assert';
import { LVar, LLam, LApp, LNum, NVar, NLam, NApp, NNum, namedToLevels, levelsToNamed, levelsToIndices, indicesToLevels, substLevel } from './debruijn-levels.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('namedToLevels: λx.x → λ.0', () => {
  const r = namedToLevels(new NLam('x', new NVar('x')));
  assert.equal(r.body.level, 0);
});

test('namedToLevels: λx.λy.x → λ.λ.0', () => {
  const r = namedToLevels(new NLam('x', new NLam('y', new NVar('x'))));
  assert.equal(r.body.body.level, 0); // x is at level 0
});

test('namedToLevels: λx.λy.y → λ.λ.1', () => {
  const r = namedToLevels(new NLam('x', new NLam('y', new NVar('y'))));
  assert.equal(r.body.body.level, 1); // y is at level 1
});

test('levelsToNamed: λ.0 → λx0.x0', () => {
  const r = levelsToNamed(new LLam(new LVar(0)));
  assert.equal(r.var, 'x0');
  assert.equal(r.body.name, 'x0');
});

test('levelsToIndices: λ.0 → λ.0 (identity stays same)', () => {
  const level = new LLam(new LVar(0));
  const index = levelsToIndices(level);
  assert.equal(index.body.level, 0); // depth-1-0 = 0
});

test('levelsToIndices: λ.λ.0 → λ.λ.1 (outer var)', () => {
  const level = new LLam(new LLam(new LVar(0)));
  const index = levelsToIndices(level);
  assert.equal(index.body.body.level, 1); // 2-1-0 = 1
});

test('indicesToLevels: roundtrip', () => {
  const original = new LLam(new LLam(new LVar(0))); // level 0 = outer
  const indices = levelsToIndices(original);
  const back = indicesToLevels(indices);
  assert.equal(back.body.body.level, 0);
});

test('substLevel: replace var', () => {
  const expr = new LApp(new LVar(0), new LVar(1));
  const result = substLevel(expr, 0, new LNum(42));
  assert.equal(result.fn.n, 42);
  assert.equal(result.arg.level, 1);
});

test('substLevel: inside lambda (no shifting needed!)', () => {
  const expr = new LLam(new LApp(new LVar(0), new LVar(1)));
  const result = substLevel(expr, 0, new LNum(42));
  assert.equal(result.body.fn.n, 42);
});

test('application roundtrip: named → levels → named', () => {
  const named = new NLam('f', new NLam('x', new NApp(new NVar('f'), new NVar('x'))));
  const levels = namedToLevels(named);
  const back = levelsToNamed(levels);
  assert.equal(back.body.body.fn.name, 'x0'); // f at level 0
});

console.log(`\nDe Bruijn levels tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
