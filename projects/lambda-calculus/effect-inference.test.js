import { strict as assert } from 'assert';
import { ENum, EPerform, ESeq, EHandle, ELet, EVar, EffectInferrer } from './effect-inference.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

function inferStr(expr) { const i = new EffectInferrer(); return i.effectString(i.infer(expr)); }

test('pure: number', () => assert.equal(inferStr(new ENum(42)), '{}'));
test('single: perform IO', () => assert.equal(inferStr(new EPerform('IO', 'print')), '{IO}'));
test('single: perform State', () => assert.equal(inferStr(new EPerform('State', 'get')), '{State}'));

test('seq: IO then State → {IO, State}', () => {
  const r = inferStr(new ESeq(new EPerform('IO', 'print'), new EPerform('State', 'get')));
  assert.ok(r.includes('IO') && r.includes('State'));
});

test('seq: same effect deduped', () => {
  const r = inferStr(new ESeq(new EPerform('IO', 'print'), new EPerform('IO', 'read')));
  assert.equal(r, '{IO}');
});

test('handle: removes handled effect', () => {
  const body = new ESeq(new EPerform('IO', 'print'), new EPerform('State', 'get'));
  const r = inferStr(new EHandle(body, 'IO', {}));
  assert.equal(r, '{State}');
});

test('handle: all effects → pure', () => {
  const r = inferStr(new EHandle(new EPerform('IO', 'print'), 'IO', {}));
  assert.equal(r, '{}');
});

test('let: propagates effects', () => {
  const r = inferStr(new ELet('x', new EPerform('IO', 'read'), new EVar('x')));
  assert.equal(r, '{IO}');
});

test('nested handles', () => {
  const body = new ESeq(new EPerform('IO', 'p'), new ESeq(new EPerform('State', 'g'), new EPerform('Exn', 't')));
  const h1 = new EHandle(body, 'IO', {});
  const h2 = new EHandle(h1, 'State', {});
  assert.equal(inferStr(h2), '{Exn}');
});

test('pure seq: pure + pure = pure', () => {
  assert.equal(inferStr(new ESeq(new ENum(1), new ENum(2))), '{}');
});

console.log(`\nEffect inference tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
