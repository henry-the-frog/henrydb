import { strict as assert } from 'assert';
import { Fun, Case, Var, Num, Con, Call, BinOp, specialize, callSiteTransform, estimateSaving } from './constructor-spec.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

const maybeFn = new Fun('fromMaybe', 'x', [
  new Case('Just', ['n'], new Var('n')),
  new Case('Nothing', [], new Num(0))
]);

test('specialize: creates specialized functions', () => {
  const { specialized } = specialize(maybeFn);
  assert.equal(specialized.length, 2);
  assert.equal(specialized[0].name, 'fromMaybe_Just');
  assert.equal(specialized[1].name, 'fromMaybe_Nothing');
});

test('specialize: Just version has n param', () => {
  const { specialized } = specialize(maybeFn);
  assert.deepStrictEqual(specialized[0].params, ['n']);
});

test('specialize: Nothing version has no params', () => {
  const { specialized } = specialize(maybeFn);
  assert.deepStrictEqual(specialized[1].params, []);
});

test('specialize: wrapper dispatches', () => {
  const { wrapper } = specialize(maybeFn);
  assert.equal(wrapper.dispatch.length, 2);
  assert.equal(wrapper.dispatch[0].call.fn, 'fromMaybe_Just');
});

test('callSiteTransform: known constructor', () => {
  const specMap = new Map([['f_Just', true]]);
  const call = new Call('f', new Con('Just', [new Num(42)]));
  const r = callSiteTransform(call, specMap);
  assert.equal(r.fn, 'f_Just');
});

test('callSiteTransform: unknown → unchanged', () => {
  const call = new Call('f', new Var('x'));
  const r = callSiteTransform(call, new Map());
  assert.equal(r.fn, 'f');
});

test('estimateSaving: counts cases', () => {
  const r = estimateSaving(maybeFn);
  assert.equal(r.totalCases, 2);
  assert.ok(r.savings.every(s => s.eliminatedPatternMatch));
});

test('specialize: body preserved', () => {
  const { specialized } = specialize(maybeFn);
  assert.equal(specialized[0].body.name, 'n');
  assert.equal(specialized[1].body.n, 0);
});

// Three-constructor type
const eitherFn = new Fun('handle', 'x', [
  new Case('Left', ['e'], new Var('e')),
  new Case('Right', ['v'], new Var('v')),
  new Case('Both', ['e', 'v'], new BinOp('+', new Var('e'), new Var('v')))
]);

test('specialize: 3 constructors', () => {
  assert.equal(specialize(eitherFn).specialized.length, 3);
});

test('specialize: Both has 2 params', () => {
  const { specialized } = specialize(eitherFn);
  assert.deepStrictEqual(specialized[2].params, ['e', 'v']);
});

console.log(`\nConstructor specialization tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
