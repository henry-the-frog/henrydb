import { strict as assert } from 'assert';
import { Var, Lam, App, Num, Add, SECD, Krivine, ZAM } from './abstract-machines.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

// Identity: (λx.x) 42
const id42 = new App(new Lam(new Var(0)), new Num(42));
// K: (λx.λy.x) 1 2
const K12 = new App(new App(new Lam(new Lam(new Var(1))), new Num(1)), new Num(2));
// Addition: 3 + 4
const add34 = new Add(new Num(3), new Num(4));

// SECD
test('SECD: number', () => assert.equal(new SECD().run(new Num(42)), 42));
test('SECD: identity', () => assert.equal(new SECD().run(id42), 42));
test('SECD: addition', () => assert.equal(new SECD().run(add34), 7));
test('SECD: K combinator', () => assert.equal(new SECD().run(K12), 1));

// Krivine
test('Krivine: number', () => assert.equal(new Krivine().run(new Num(42)), 42));
test('Krivine: identity', () => assert.equal(new Krivine().run(id42), 42));
test('Krivine: addition', () => assert.equal(new Krivine().run(add34), 7));

// ZAM
test('ZAM: number', () => assert.equal(new ZAM().run(new Num(42)), 42));
test('ZAM: identity', () => assert.equal(new ZAM().run(id42), 42));
test('ZAM: addition', () => assert.equal(new ZAM().run(add34), 7));

// All three agree
test('all machines agree on (λx.x) 42', () => {
  assert.equal(new SECD().run(id42), new Krivine().run(id42));
  assert.equal(new Krivine().run(id42), new ZAM().run(id42));
});

console.log(`\nAbstract machines tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
