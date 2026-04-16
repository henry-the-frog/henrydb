import { strict as assert } from 'assert';
import { Identity, MaybeT, StateT, ReaderT } from './monad-transformers.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('Identity: of + chain', () => assert.equal(Identity.of(42).chain(x => Identity.of(x + 1)).value, 43));

test('MaybeT: of wraps in Just', () => {
  const r = MaybeT.of(Identity)(42).run;
  assert.equal(r.value.tag, 'Just');
  assert.equal(r.value.value, 42);
});

test('MaybeT: nothing', () => {
  const r = MaybeT.nothing(Identity).run;
  assert.equal(r.value.tag, 'Nothing');
});

test('MaybeT: chain Just', () => {
  const r = MaybeT.of(Identity)(21).chain(Identity, x => MaybeT.of(Identity)(x * 2)).run;
  assert.equal(r.value.value, 42);
});

test('MaybeT: chain Nothing short-circuits', () => {
  const r = MaybeT.nothing(Identity).chain(Identity, x => MaybeT.of(Identity)(x * 2)).run;
  assert.equal(r.value.tag, 'Nothing');
});

test('StateT: of preserves state', () => {
  const [v, s] = StateT.of(Identity)(42).exec('init').value;
  assert.equal(v, 42);
  assert.equal(s, 'init');
});

test('StateT: get reads state', () => {
  const [v, s] = StateT.get(Identity).exec('hello').value;
  assert.equal(v, 'hello');
});

test('StateT: put sets state', () => {
  const [_, s] = StateT.put(Identity, 'new').exec('old').value;
  assert.equal(s, 'new');
});

test('ReaderT: of ignores env', () => {
  const r = ReaderT.of(Identity)(42).run('anything');
  assert.equal(r.value, 42);
});

test('ReaderT: ask reads env', () => {
  const r = ReaderT.ask(Identity).run('config');
  assert.equal(r.value, 'config');
});

console.log(`\nMonad transformers tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
