import { strict as assert } from 'assert';
import { SVar, SNum, SApp, SLam, SLet, SHole, SIf, elaborate, findHoles } from './bidir-elaboration.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('elaborate: num', () => assert.equal(elaborate(new SNum(42)).type, 'Int'));
test('elaborate: var', () => {
  const r = elaborate(new SVar('x'), new Map([['x', 'Int']]));
  assert.equal(r.type, 'Int');
});
test('elaborate: lambda', () => {
  const r = elaborate(new SLam('x', new SVar('x')), new Map(), 'Int → Int');
  assert.equal(r.tag, 'CLam');
  assert.equal(r.paramType, 'Int');
});
test('elaborate: application', () => {
  const env = new Map([['f', 'Int → Bool'], ['x', 'Int']]);
  const r = elaborate(new SApp(new SVar('f'), new SVar('x')), env);
  assert.equal(r.type, 'Bool');
});
test('elaborate: let', () => {
  const r = elaborate(new SLet('x', new SNum(42), new SVar('x')));
  assert.equal(r.body.type, 'Int');
});
test('elaborate: hole', () => {
  const r = elaborate(new SHole(), new Map(), 'Int');
  assert.equal(r.tag, 'CHole');
  assert.ok(r.message.includes('Int'));
});
test('elaborate: if', () => {
  const env = new Map([['b', 'Bool']]);
  const r = elaborate(new SIf(new SVar('b'), new SNum(1), new SNum(2)), env);
  assert.equal(r.type, 'Int');
});
test('findHoles: none', () => {
  assert.equal(findHoles(elaborate(new SNum(42))).length, 0);
});
test('findHoles: with hole', () => {
  const r = elaborate(new SLet('x', new SHole(), new SNum(1)), new Map(), 'Int');
  assert.equal(findHoles(r).length, 1);
});
test('elaborate: nested', () => {
  const expr = new SLet('id', new SLam('x', new SVar('x')), new SApp(new SVar('id'), new SNum(42)));
  const r = elaborate(expr);
  assert.equal(r.tag, 'CLet');
});

console.log(`\nBidirectional elaboration tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
