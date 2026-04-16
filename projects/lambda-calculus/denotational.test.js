import { strict as assert } from 'assert';
import { denote, fix, Bot, lift, N, B, V, Add, Mul, Lam, App, Let, If, Seq, Unit } from './denotational.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('num: ⟦42⟧ = 42', () => assert.equal(denote(N(42)), 42));
test('add: ⟦2+3⟧ = 5', () => assert.equal(denote(Add(N(2), N(3))), 5));
test('mul: ⟦4*5⟧ = 20', () => assert.equal(denote(Mul(N(4), N(5))), 20));
test('lam+app: ⟦(λx.x) 42⟧ = 42', () => assert.equal(denote(App(Lam('x', V('x')), N(42))), 42));
test('let: ⟦let x=5 in x+1⟧ = 6', () => assert.equal(denote(Let('x', N(5), Add(V('x'), N(1)))), 6));
test('if: ⟦if true then 1 else 2⟧ = 1', () => assert.equal(denote(If(B(true), N(1), N(2))), 1));
test('K: ⟦K 1 2⟧ = 1', () => assert.equal(denote(App(App(Lam('x', Lam('y', V('x'))), N(1)), N(2))), 1));
test('seq: ⟦(); 42⟧ = 42', () => assert.equal(denote(Seq(Unit(), N(42))), 42));
test('lift: preserves bottom', () => assert.equal(lift(x => x + 1)(Bot), Bot));
test('lift: applies to value', () => assert.equal(lift(x => x + 1)(5), 6));
test('fix: converges', () => {
  const f = x => x === null ? 0 : x + 1;
  assert.ok(fix(f) > 0);
});

console.log(`\n🎉🎉🎉 MODULE #180!!! Denotational semantics tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
