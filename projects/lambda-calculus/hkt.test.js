import { strict as assert } from 'assert';
import {
  kStar, kStarToStar, kStarStarToStar, kindEquals, KArrow,
  tInt, tBool, tStr, tList, tMaybe, tIO, tEither, tPair,
  listOf, maybeOf, ioOf, eitherOf, pairOf,
  inferKind,
  listFunctor, maybeFunctor, eitherFunctor,
  listMonad, maybeMonad,
  fmap, mreturn, mbind,
  checkLeftIdentity, checkRightIdentity, checkAssociativity
} from './hkt.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// ============================================================
// Kind system
// ============================================================

test('Int has kind ★', () => assert.ok(kindEquals(inferKind(tInt), kStar)));
test('Bool has kind ★', () => assert.ok(kindEquals(inferKind(tBool), kStar)));
test('List has kind ★ → ★', () => assert.ok(kindEquals(inferKind(tList), kStarToStar)));
test('Maybe has kind ★ → ★', () => assert.ok(kindEquals(inferKind(tMaybe), kStarToStar)));
test('Either has kind ★ → ★ → ★', () => assert.ok(kindEquals(inferKind(tEither), kStarStarToStar)));

test('List Int has kind ★', () => assert.ok(kindEquals(inferKind(listOf(tInt)), kStar)));
test('Maybe String has kind ★', () => assert.ok(kindEquals(inferKind(maybeOf(tStr)), kStar)));
test('Either Int Bool has kind ★', () => assert.ok(kindEquals(inferKind(eitherOf(tInt, tBool)), kStar)));
test('Either Int has kind ★ → ★', () => {
  const partial = eitherOf(tInt, tBool);
  // Actually Either Int is ★ → ★, but eitherOf applies both args
  // Let's test partial application manually
  const eitherInt = { tag: 'TApp', con: tEither, arg: tInt };
  assert.ok(kindEquals(inferKind(eitherInt), kStarToStar));
});

test('kind error: Int Int', () => {
  assert.throws(() => inferKind({ tag: 'TApp', con: tInt, arg: tInt }), /arrow kind/);
});

// ============================================================
// Functor
// ============================================================

test('List fmap: [1,2,3] → [2,3,4]', () => {
  const result = fmap(listFunctor, x => x + 1, [1, 2, 3]);
  assert.deepStrictEqual(result, [2, 3, 4]);
});

test('Maybe fmap Just: Just 5 → Just 10', () => {
  const result = fmap(maybeFunctor, x => x * 2, { tag: 'Just', value: 5 });
  assert.deepStrictEqual(result, { tag: 'Just', value: 10 });
});

test('Maybe fmap Nothing: Nothing → Nothing', () => {
  const nothing = { tag: 'Nothing' };
  const result = fmap(maybeFunctor, x => x * 2, nothing);
  assert.deepStrictEqual(result, nothing);
});

test('Either fmap Right: Right 5 → Right 10', () => {
  const result = fmap(eitherFunctor, x => x * 2, { tag: 'Right', value: 5 });
  assert.deepStrictEqual(result, { tag: 'Right', value: 10 });
});

test('Either fmap Left: Left "err" → Left "err"', () => {
  const left = { tag: 'Left', value: 'err' };
  assert.deepStrictEqual(fmap(eitherFunctor, x => x * 2, left), left);
});

// ============================================================
// Monad
// ============================================================

test('List return', () => {
  assert.deepStrictEqual(mreturn(listMonad, 42), [42]);
});

test('List bind: flatMap', () => {
  const result = mbind(listMonad, [1, 2, 3], x => [x, x * 10]);
  assert.deepStrictEqual(result, [1, 10, 2, 20, 3, 30]);
});

test('Maybe return', () => {
  assert.deepStrictEqual(mreturn(maybeMonad, 42), { tag: 'Just', value: 42 });
});

test('Maybe bind Just', () => {
  const result = mbind(maybeMonad, { tag: 'Just', value: 5 }, x => ({ tag: 'Just', value: x + 1 }));
  assert.deepStrictEqual(result, { tag: 'Just', value: 6 });
});

test('Maybe bind Nothing', () => {
  const result = mbind(maybeMonad, { tag: 'Nothing' }, x => ({ tag: 'Just', value: x + 1 }));
  assert.deepStrictEqual(result, { tag: 'Nothing' });
});

// ============================================================
// Monad Laws
// ============================================================

test('List: left identity', () => {
  assert.ok(checkLeftIdentity(listMonad, 5, x => [x, x + 1]));
});

test('List: right identity', () => {
  assert.ok(checkRightIdentity(listMonad, [1, 2, 3]));
});

test('List: associativity', () => {
  assert.ok(checkAssociativity(
    listMonad, [1, 2],
    x => [x, x * 10],
    x => [x + 1]
  ));
});

test('Maybe: left identity', () => {
  assert.ok(checkLeftIdentity(maybeMonad, 5, x => ({ tag: 'Just', value: x * 2 })));
});

test('Maybe: right identity', () => {
  assert.ok(checkRightIdentity(maybeMonad, { tag: 'Just', value: 42 }));
});

test('Maybe: associativity', () => {
  assert.ok(checkAssociativity(
    maybeMonad, { tag: 'Just', value: 5 },
    x => ({ tag: 'Just', value: x * 2 }),
    x => ({ tag: 'Just', value: x + 1 })
  ));
});

// ============================================================
// Practical: monadic computation
// ============================================================

test('List comprehension via bind: pairs', () => {
  // [(x, y) | x ← [1,2], y ← [10,20]]
  const result = mbind(listMonad, [1, 2], x =>
    mbind(listMonad, [10, 20], y =>
      mreturn(listMonad, [x, y])));
  assert.deepStrictEqual(result, [[1,10],[1,20],[2,10],[2,20]]);
});

test('Maybe chaining: safe division', () => {
  const safeDiv = (a, b) => b === 0 ? { tag: 'Nothing' } : { tag: 'Just', value: Math.floor(a / b) };
  
  // 100 / 10 / 2 = 5
  const result = mbind(maybeMonad, safeDiv(100, 10), x =>
    mbind(maybeMonad, safeDiv(x, 2), y =>
      mreturn(maybeMonad, y)));
  assert.deepStrictEqual(result, { tag: 'Just', value: 5 });
  
  // 100 / 0 = Nothing (short-circuits)
  const result2 = mbind(maybeMonad, safeDiv(100, 0), x =>
    mreturn(maybeMonad, x + 1));
  assert.deepStrictEqual(result2, { tag: 'Nothing' });
});

// ============================================================
// Report
// ============================================================

console.log(`\nHKT tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
