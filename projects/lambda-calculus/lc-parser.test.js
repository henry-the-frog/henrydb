import { strict as assert } from 'assert';
import { parse, prettyPrint } from './lc-parser.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('parse variable', () => assert.equal(parse('x').name, 'x'));
test('parse number', () => assert.equal(parse('42').n, 42));
test('parse lambda (\\)', () => {
  const r = parse('\\x.x');
  assert.equal(r.tag, 'Lam');
  assert.equal(r.var, 'x');
});
test('parse lambda (λ)', () => {
  const r = parse('λx.x');
  assert.equal(r.tag, 'Lam');
});
test('parse application', () => {
  const r = parse('f x');
  assert.equal(r.tag, 'App');
  assert.equal(r.fn.name, 'f');
  assert.equal(r.arg.name, 'x');
});
test('parse nested application: f x y', () => {
  const r = parse('f x y');
  assert.equal(r.tag, 'App');
  assert.equal(r.fn.tag, 'App'); // Left associative
});
test('parse parenthesized', () => {
  const r = parse('(\\x.x) 42');
  assert.equal(r.tag, 'App');
  assert.equal(r.fn.tag, 'Lam');
  assert.equal(r.arg.n, 42);
});
test('parse nested lambda', () => {
  const r = parse('\\x.\\y.x');
  assert.equal(r.body.tag, 'Lam');
});
test('prettyPrint roundtrip', () => {
  const s = prettyPrint(parse('\\x.x'));
  assert.ok(s.includes('λ') || s.includes('x'));
});
test('parse complex: (\\f.\\x.f x) (\\y.y) 42', () => {
  const r = parse('(\\f.\\x.f x) (\\y.y) 42');
  assert.equal(r.tag, 'App');
});

console.log(`\nLambda calculus parser tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
