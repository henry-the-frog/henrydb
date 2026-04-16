import { strict as assert } from 'assert';
import {
  GDyn, GInt, GBool, GStr, GFun, GPair, GList,
  dyn, gint, gbool, gstr,
  consistent, isStatic, isGround, typeEquals,
  Cast, evalCast, CastError, freshBlame, resetBlame, runtimeTypeCheck,
  meet, join
} from './gradual.js';

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
// Consistency
// ============================================================

test('? ~ Int', () => assert.ok(consistent(dyn, gint)));
test('Int ~ ?', () => assert.ok(consistent(gint, dyn)));
test('? ~ ?', () => assert.ok(consistent(dyn, dyn)));
test('Int ~ Int', () => assert.ok(consistent(gint, gint)));
test('Int !~ Bool', () => assert.ok(!consistent(gint, gbool)));
test('Int !~ Str', () => assert.ok(!consistent(gint, gstr)));

test('(? → ?) ~ (Int → Bool)', () => {
  assert.ok(consistent(new GFun(dyn, dyn), new GFun(gint, gbool)));
});

test('(Int → ?) ~ (Int → Bool)', () => {
  assert.ok(consistent(new GFun(gint, dyn), new GFun(gint, gbool)));
});

test('(Int → Int) !~ (Bool → Bool)', () => {
  assert.ok(!consistent(new GFun(gint, gint), new GFun(gbool, gbool)));
});

test('[?] ~ [Int]', () => {
  assert.ok(consistent(new GList(dyn), new GList(gint)));
});

// ============================================================
// Static / Ground
// ============================================================

test('Int is static', () => assert.ok(isStatic(gint)));
test('? is not static', () => assert.ok(!isStatic(dyn)));
test('Int → ? is not static', () => assert.ok(!isStatic(new GFun(gint, dyn))));
test('Int → Int is static', () => assert.ok(isStatic(new GFun(gint, gint))));

test('Int is ground', () => assert.ok(isGround(gint)));
test('? → ? is ground', () => assert.ok(isGround(new GFun(dyn, dyn))));
test('Int → Int is not ground', () => assert.ok(!isGround(new GFun(gint, gint))));
test('[?] is ground', () => assert.ok(isGround(new GList(dyn))));

// ============================================================
// Type Equality
// ============================================================

test('Int == Int', () => assert.ok(typeEquals(gint, gint)));
test('Int != Bool', () => assert.ok(!typeEquals(gint, gbool)));
test('? == ?', () => assert.ok(typeEquals(dyn, dyn)));
test('? != Int', () => assert.ok(!typeEquals(dyn, gint)));
test('(Int → Bool) == (Int → Bool)', () => {
  assert.ok(typeEquals(new GFun(gint, gbool), new GFun(gint, gbool)));
});

// ============================================================
// Cast Evaluation
// ============================================================

test('identity cast: Int to Int', () => {
  const result = evalCast(42, gint, gint, 'test');
  assert.equal(result, 42);
});

test('cast Int to ?', () => {
  const result = evalCast(42, gint, dyn, 'test');
  assert.equal(result, 42);
});

test('cast ? to Int (success)', () => {
  const result = evalCast(42, dyn, gint, 'test');
  assert.equal(result, 42);
});

test('cast ? to Int (failure: string)', () => {
  assert.throws(() => evalCast('hello', dyn, gint, 'blame1'), CastError);
});

test('cast ? to Bool (success)', () => {
  assert.equal(evalCast(true, dyn, gbool, 'test'), true);
});

test('cast ? to Bool (failure: number)', () => {
  assert.throws(() => evalCast(42, dyn, gbool, 'blame2'), CastError);
});

test('cast ? to Str (success)', () => {
  assert.equal(evalCast('hi', dyn, gstr, 'test'), 'hi');
});

test('cast blame tracking', () => {
  try {
    evalCast('hello', dyn, gint, 'line42');
    assert.fail('Should have thrown');
  } catch (e) {
    assert.ok(e instanceof CastError);
    assert.equal(e.blame, 'line42');
  }
});

// ============================================================
// Runtime Type Checking
// ============================================================

test('runtimeTypeCheck: 42 is Int', () => assert.ok(runtimeTypeCheck(42, gint)));
test('runtimeTypeCheck: 3.14 is not Int', () => assert.ok(!runtimeTypeCheck(3.14, gint)));
test('runtimeTypeCheck: true is Bool', () => assert.ok(runtimeTypeCheck(true, gbool)));
test('runtimeTypeCheck: "hi" is Str', () => assert.ok(runtimeTypeCheck('hi', gstr)));
test('runtimeTypeCheck: anything is ?', () => assert.ok(runtimeTypeCheck(42, dyn)));
test('runtimeTypeCheck: function is Fun', () => {
  assert.ok(runtimeTypeCheck(() => {}, new GFun(dyn, dyn)));
});

// ============================================================
// Meet and Join
// ============================================================

test('meet(Int, ?) = Int', () => {
  assert.ok(typeEquals(meet(gint, dyn), gint));
});

test('meet(?, Int) = Int', () => {
  assert.ok(typeEquals(meet(dyn, gint), gint));
});

test('meet(?, ?) = ?', () => {
  assert.ok(typeEquals(meet(dyn, dyn), dyn));
});

test('meet(Int, Int) = Int', () => {
  assert.ok(typeEquals(meet(gint, gint), gint));
});

test('meet(Int, Bool) = null (inconsistent)', () => {
  assert.equal(meet(gint, gbool), null);
});

test('meet(Int→?, ?→Bool) = Int→Bool', () => {
  const result = meet(new GFun(gint, dyn), new GFun(dyn, gbool));
  assert.ok(typeEquals(result, new GFun(gint, gbool)));
});

test('join(Int, ?) = ?', () => {
  assert.ok(typeEquals(join(gint, dyn), dyn));
});

test('join(Int, Int) = Int', () => {
  assert.ok(typeEquals(join(gint, gint), gint));
});

test('join(Int, Bool) = ?', () => {
  assert.ok(typeEquals(join(gint, gbool), dyn));
});

// ============================================================
// Report
// ============================================================

console.log(`\nGradual type tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
