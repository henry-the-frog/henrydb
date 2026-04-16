import { strict as assert } from 'assert';
import { Var, Lam, App, bot, toHNF, bohmTree, approximate, btSize } from './bohm-trees.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const I = new Lam('x', new Var('x'));
const K = new Lam('x', new Lam('y', new Var('x')));

test('HNF: variable', () => { assert.equal(toHNF(new Var('x')).name, 'x'); });
test('HNF: lambda', () => { assert.equal(toHNF(I).tag, 'Lam'); });
test('HNF: beta reduce', () => { assert.equal(toHNF(new App(I, new Var('y'))).name, 'y'); });
test('HNF: nested beta', () => { assert.equal(toHNF(new App(new App(K, new Var('a')), new Var('b'))).name, 'a'); });

test('BT: variable → BT_Node', () => {
  const bt = bohmTree(new Var('x'));
  assert.equal(bt.tag, 'BT_Node');
  assert.equal(bt.head, 'x');
});

test('BT: identity → BT_Lam', () => {
  const bt = bohmTree(I);
  assert.equal(bt.tag, 'BT_Lam');
});

test('BT: K → λx.λy.x', () => {
  const bt = bohmTree(K);
  assert.equal(bt.tag, 'BT_Lam');
  assert.equal(bt.body.tag, 'BT_Lam');
});

test('BT: application f x → node with arg', () => {
  const bt = bohmTree(new App(new Var('f'), new Var('x')));
  assert.equal(bt.tag, 'BT_Node');
  assert.equal(bt.args.length, 1);
});

test('approximate: truncate', () => {
  const bt = bohmTree(K, 5);
  const approx = approximate(bt, 1);
  assert.equal(approx.tag, 'BT_Lam');
  assert.equal(approx.body.tag, 'BT_Bot');
});

test('btSize: bot = 1', () => assert.equal(btSize(bot), 1));
test('btSize: K > 1', () => assert.ok(btSize(bohmTree(K)) > 1));

console.log(`\nBöhm trees tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
