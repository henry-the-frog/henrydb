import { strict as assert } from 'assert';
import {
  KStar, KArrow, star,
  TCon, TApp, TVar, TArrow,
  resetKindVars, resolveKind,
  createStdEnv
} from './kind-inference.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { resetKindVars(); fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('Int : *', () => {
  const kc = createStdEnv();
  const k = resolveKind(kc.infer(new TCon('Int')));
  assert.equal(k.tag, 'KStar');
});

test('List : * → *', () => {
  const kc = createStdEnv();
  const k = resolveKind(kc.infer(new TCon('List')));
  assert.equal(k.tag, 'KArrow');
  assert.equal(resolveKind(k.param).tag, 'KStar');
  assert.equal(resolveKind(k.ret).tag, 'KStar');
});

test('List Int : *', () => {
  const kc = createStdEnv();
  const k = resolveKind(kc.infer(new TApp(new TCon('List'), new TCon('Int'))));
  assert.equal(k.tag, 'KStar');
});

test('Map : * → * → *', () => {
  const kc = createStdEnv();
  const k = resolveKind(kc.infer(new TCon('Map')));
  assert.equal(k.tag, 'KArrow');
});

test('Map String Int : *', () => {
  const kc = createStdEnv();
  const k = resolveKind(kc.infer(new TApp(new TApp(new TCon('Map'), new TCon('String')), new TCon('Int'))));
  assert.equal(k.tag, 'KStar');
});

test('Int → Bool : *', () => {
  const kc = createStdEnv();
  const k = resolveKind(kc.infer(new TArrow(new TCon('Int'), new TCon('Bool'))));
  assert.equal(k.tag, 'KStar');
});

test('Either Int : * → *', () => {
  const kc = createStdEnv();
  const k = resolveKind(kc.infer(new TApp(new TCon('Either'), new TCon('Int'))));
  assert.equal(k.tag, 'KArrow');
});

test('type variable: inferred kind', () => {
  const kc = createStdEnv();
  // List a → a should give a kind * (since List a : * requires a : *)
  kc.infer(new TApp(new TCon('List'), new TVar('a')));
  const aKind = resolveKind(kc.env.get('a'));
  assert.equal(aKind.tag, 'KStar');
});

test('unknown type: error', () => {
  const kc = createStdEnv();
  kc.infer(new TCon('Foo'));
  assert.ok(kc.errors.length > 0);
});

test('check: Int against * succeeds', () => {
  const kc = createStdEnv();
  assert.ok(kc.check(new TCon('Int'), star));
});

test('check: List against * fails', () => {
  const kc = createStdEnv();
  const result = kc.check(new TCon('List'), star);
  assert.ok(!result);
});

console.log(`\nKind inference tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
