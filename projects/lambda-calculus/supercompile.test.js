import { strict as assert } from 'assert';
import { Num, Var, Add, Mul, If0, drive, homeomorphicEmbedding, supercompile } from './supercompile.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('drive: 2+3 → 5', () => assert.equal(drive(new Add(new Num(2), new Num(3))).n, 5));
test('drive: 0+x → x', () => assert.equal(drive(new Add(new Num(0), new Var('x'))).name, 'x'));
test('drive: x*0 → 0', () => assert.equal(drive(new Mul(new Var('x'), new Num(0))).n, 0));
test('drive: x*1 → x', () => assert.equal(drive(new Mul(new Var('x'), new Num(1))).name, 'x'));
test('drive: if0(0,a,b) → a', () => assert.equal(drive(new If0(new Num(0), new Var('a'), new Var('b'))).name, 'a'));
test('drive: if0(1,a,b) → b', () => assert.equal(drive(new If0(new Num(1), new Var('a'), new Var('b'))).name, 'b'));

test('homeomorphic: Num embeds in Add', () => {
  assert.ok(homeomorphicEmbedding(new Num(1), new Add(new Num(1), new Num(2))));
});

test('supercompile: (0+x)*(1+0) → x', () => {
  const expr = new Mul(new Add(new Num(0), new Var('x')), new Add(new Num(1), new Num(0)));
  const r = supercompile(expr);
  assert.equal(r.result.name, 'x');
});

test('supercompile: nested constants', () => {
  const expr = new Add(new Mul(new Num(2), new Num(3)), new Num(4));
  const r = supercompile(expr);
  assert.equal(r.result.n, 10);
});

test('supercompile: already simple', () => {
  const r = supercompile(new Var('x'));
  assert.equal(r.result.name, 'x');
  assert.equal(r.steps, 1);
});

console.log(`\nSupercompilation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
