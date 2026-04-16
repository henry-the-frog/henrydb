import { strict as assert } from 'assert';
import { Var, Lam, App, step, normalize, checkConfluence, isNormal, eq } from './church-rosser.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const x = new Var('x'), y = new Var('y'), z = new Var('z');
const I = new Var('i'), K2 = new Lam('a', new Lam('b', new Var('a')));

test('step: (λx.x) y → y', () => assert.ok(eq(step(new App(new Lam('x', x), y)), y)));
test('isNormal: var', () => assert.ok(isNormal(x)));
test('isNormal: lambda', () => assert.ok(isNormal(new Lam('x', x))));
test('isNormal: redex → false', () => assert.ok(!isNormal(new App(new Lam('x', x), y))));
test('normalize: (λx.x) y → y', () => assert.ok(eq(normalize(new App(new Lam('x', x), y)), y)));
test('normalize: K a b → a', () => assert.ok(eq(normalize(new App(new App(K2, y), z)), y)));

// Church-Rosser: different reduction orders converge
test('confluence: (K I) y and K I y same normal form', () => {
  const e1 = new App(new App(K2, new Lam('x', x)), y);
  const e2 = new App(new App(K2, new Lam('x', x)), z);
  const n1 = normalize(e1), n2 = normalize(e2);
  // Both reduce to I
  assert.ok(eq(n1, n2));
});

test('eq: same terms', () => assert.ok(eq(new App(x, y), new App(x, y))));
test('eq: different terms', () => assert.ok(!eq(x, y)));
test('normalize: already normal', () => assert.ok(eq(normalize(x), x)));

console.log(`\nChurch-Rosser tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
