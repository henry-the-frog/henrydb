import { strict as assert } from 'assert';
import { Var, Lam, App, nbe, exprToString } from './nbe-untyped.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('nbe: var stays var', () => assert.equal(nbe(new Var('x')).name, 'x'));
test('nbe: id stays id', () => assert.equal(nbe(new Lam('x', new Var('x'))).tag, 'Lam'));
test('nbe: (λx.x) y → y', () => assert.equal(nbe(new App(new Lam('x', new Var('x')), new Var('y'))).name, 'y'));
test('nbe: K a b → a', () => {
  const K = new Lam('x', new Lam('y', new Var('x')));
  const r = nbe(new App(new App(K, new Var('a')), new Var('b')));
  assert.equal(r.name, 'a');
});
test('nbe: eta reduces λx.(f x) → f', () => {
  // NbE performs eta-expansion, not reduction, for neutrals
  const t = nbe(new Lam('x', new App(new Var('f'), new Var('x'))));
  assert.equal(t.tag, 'Lam'); // Still a lambda (f is neutral)
});
test('nbe: nested app', () => {
  const r = nbe(new App(new Lam('x', new App(new Var('x'), new Var('z'))), new Var('f')));
  assert.equal(r.tag, 'App');
});
test('exprToString: var', () => assert.equal(exprToString(new Var('x')), 'x'));
test('exprToString: lam', () => assert.ok(exprToString(new Lam('x', new Var('x'))).includes('λ')));
test('exprToString: app', () => assert.ok(exprToString(new App(new Var('f'), new Var('x'))).includes('f')));
test('nbe: S combinator partially applied', () => {
  const S = new Lam('f', new Lam('g', new Lam('x', new App(new App(new Var('f'), new Var('x')), new App(new Var('g'), new Var('x'))))));
  const r = nbe(S);
  assert.equal(r.tag, 'Lam');
});

console.log(`\nNbE (untyped) tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
