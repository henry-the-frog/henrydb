import { strict as assert } from 'assert';
import { AGNode, AttributeGrammar, makeEvalAG, makeDepthAG } from './attribute-grammar.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('eval AG: num', () => {
  const ag = makeEvalAG();
  const tree = new AGNode('Num', [], { n: 42 });
  ag.evaluate(tree);
  assert.equal(tree.synth.value, 42);
});

test('eval AG: 2+3=5', () => {
  const ag = makeEvalAG();
  const tree = new AGNode('Add', [new AGNode('Num', [], { n: 2 }), new AGNode('Num', [], { n: 3 })]);
  ag.evaluate(tree);
  assert.equal(tree.synth.value, 5);
});

test('eval AG: 4*5=20', () => {
  const ag = makeEvalAG();
  const tree = new AGNode('Mul', [new AGNode('Num', [], { n: 4 }), new AGNode('Num', [], { n: 5 })]);
  ag.evaluate(tree);
  assert.equal(tree.synth.value, 20);
});

test('eval AG: neg', () => {
  const ag = makeEvalAG();
  const tree = new AGNode('Neg', [new AGNode('Num', [], { n: 7 })]);
  ag.evaluate(tree);
  assert.equal(tree.synth.value, -7);
});

test('eval AG: (2+3)*4=20', () => {
  const ag = makeEvalAG();
  const tree = new AGNode('Mul', [
    new AGNode('Add', [new AGNode('Num', [], { n: 2 }), new AGNode('Num', [], { n: 3 })]),
    new AGNode('Num', [], { n: 4 })
  ]);
  ag.evaluate(tree);
  assert.equal(tree.synth.value, 20);
});

test('depth AG: leaf', () => {
  const ag = makeDepthAG();
  const tree = new AGNode('Leaf');
  ag.evaluate(tree);
  assert.equal(tree.synth.depth, 0);
});

test('depth AG: single node', () => {
  const ag = makeDepthAG();
  const tree = new AGNode('Node', [new AGNode('Leaf'), new AGNode('Leaf')]);
  ag.evaluate(tree);
  assert.equal(tree.synth.depth, 1);
});

test('depth AG: nested', () => {
  const ag = makeDepthAG();
  const tree = new AGNode('Node', [
    new AGNode('Node', [new AGNode('Leaf')]),
    new AGNode('Leaf')
  ]);
  ag.evaluate(tree);
  assert.equal(tree.synth.depth, 2);
});

test('inherited attributes propagate', () => {
  const ag = new AttributeGrammar();
  ag.addInhRule('Root', 0, 'level', () => 1);
  ag.addSynthRule('Child', 'result', n => n.inh.level || 0);
  const tree = new AGNode('Root', [new AGNode('Child')]);
  ag.evaluate(tree);
  assert.equal(tree.children[0].synth.result, 1);
});

test('custom AG: count nodes', () => {
  const ag = new AttributeGrammar();
  ag.addSynthRule('Leaf', 'count', () => 1);
  ag.addSynthRule('Node', 'count', n => 1 + n.children.reduce((s, c) => s + c.synth.count, 0));
  const tree = new AGNode('Node', [new AGNode('Leaf'), new AGNode('Node', [new AGNode('Leaf')])]);
  ag.evaluate(tree);
  assert.equal(tree.synth.count, 4);
});

console.log(`\nAttribute grammar tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
