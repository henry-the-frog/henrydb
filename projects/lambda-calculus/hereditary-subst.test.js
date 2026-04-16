import { strict as assert } from 'assert';
import { Var, Lam, App, shift, standardSubst, hereditarySubst, isNormal, countRedexes } from './hereditary-subst.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('shift: free var +1', () => assert.equal(shift(new Var(0), 1).idx, 1));
test('shift: bound var unchanged', () => assert.equal(shift(new Lam(new Var(0)), 1).body.idx, 0));

test('standard: [x ↦ y] x → y', () => {
  const r = standardSubst(new Var(0), 0, new Var(42));
  assert.equal(r.idx, 42);
});

test('standard: may create redex', () => {
  // (λ.0) substituted into App position creates a redex
  const body = new App(new Var(0), new Var(1)); // 0 applied to 1
  const repl = new Lam(new Var(0)); // identity
  const result = standardSubst(body, 0, repl);
  assert.ok(countRedexes(result) > 0); // Has a redex
});

test('hereditary: same result but normalized', () => {
  // Same as above but hereditary eliminates the redex
  const body = new App(new Var(0), new Var(1));
  const repl = new Lam(new Var(0));
  const result = hereditarySubst(body, 0, repl);
  assert.ok(isNormal(result)); // No redexes!
});

test('hereditary: simple subst', () => {
  const r = hereditarySubst(new Var(0), 0, new Var(42));
  assert.equal(r.idx, 42);
});

test('isNormal: var → true', () => assert.ok(isNormal(new Var(0))));
test('isNormal: lam → true', () => assert.ok(isNormal(new Lam(new Var(0)))));
test('isNormal: redex → false', () => assert.ok(!isNormal(new App(new Lam(new Var(0)), new Var(1)))));

test('countRedexes: 1 redex', () => assert.equal(countRedexes(new App(new Lam(new Var(0)), new Var(1))), 1));
test('countRedexes: 0 in normal', () => assert.equal(countRedexes(new App(new Var(0), new Var(1))), 0));

console.log(`\nHereditary substitution tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
