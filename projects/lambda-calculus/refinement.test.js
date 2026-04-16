import { strict as assert } from 'assert';
import {
  RBase, RRefined, RFun, RList, rInt, rBool, rStr, rUnit,
  pvar, pnum, pbool, pgt, pge, plt, ple, peq, pand, por, pmod, pnot,
  formatPred,
  posInt, natType, evenInt, boundedInt, nonEmpty,
  isSubtype, checkValue, evalPredicate
} from './refinement.js';

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
// Type constructors
// ============================================================

test('posInt type', () => {
  const t = posInt();
  assert.equal(t.toString(), '{x:Int | x > 0}');
});

test('natType type', () => {
  const t = natType();
  assert.equal(t.toString(), '{x:Int | x >= 0}');
});

test('evenInt type', () => {
  const t = evenInt();
  assert.ok(t.toString().includes('x % 2 == 0'));
});

test('boundedInt type', () => {
  const t = boundedInt(0, 255);
  assert.ok(t.toString().includes('0'));
  assert.ok(t.toString().includes('255'));
});

// ============================================================
// Value checking
// ============================================================

test('5 : posInt', () => assert.ok(checkValue(5, posInt())));
test('0 : not posInt', () => assert.ok(!checkValue(0, posInt())));
test('-1 : not posInt', () => assert.ok(!checkValue(-1, posInt())));

test('0 : natType', () => assert.ok(checkValue(0, natType())));
test('-1 : not natType', () => assert.ok(!checkValue(-1, natType())));

test('4 : evenInt', () => assert.ok(checkValue(4, evenInt())));
test('3 : not evenInt', () => assert.ok(!checkValue(3, evenInt())));

test('100 : boundedInt(0, 255)', () => assert.ok(checkValue(100, boundedInt(0, 255))));
test('256 : not boundedInt(0, 255)', () => assert.ok(!checkValue(256, boundedInt(0, 255))));
test('-1 : not boundedInt(0, 255)', () => assert.ok(!checkValue(-1, boundedInt(0, 255))));

test('42 : Int', () => assert.ok(checkValue(42, rInt)));
test('"hello" : Str', () => assert.ok(checkValue('hello', rStr)));
test('true : Bool', () => assert.ok(checkValue(true, rBool)));

// ============================================================
// Subtyping
// ============================================================

test('Int <: Int', () => {
  assert.ok(isSubtype(rInt, rInt).isSubtype);
});

test('Int !<: Bool', () => {
  assert.ok(!isSubtype(rInt, rBool).isSubtype);
});

test('posInt <: Int (refined subtype of base)', () => {
  assert.ok(isSubtype(posInt(), rInt).isSubtype);
});

test('posInt <: natType (x>0 ⇒ x>=0)', () => {
  const result = isSubtype(posInt(), natType());
  assert.ok(result.isSubtype, result.reason);
});

test('natType !<: posInt (x>=0 does not imply x>0)', () => {
  const result = isSubtype(natType(), posInt());
  assert.ok(!result.isSubtype);
});

test('{x:Int | x > 5} <: {x:Int | x > 3}', () => {
  const t1 = new RRefined('x', rInt, pgt(pvar('x'), pnum(5)));
  const t2 = new RRefined('x', rInt, pgt(pvar('x'), pnum(3)));
  assert.ok(isSubtype(t1, t2).isSubtype);
});

test('{x:Int | x > 3} !<: {x:Int | x > 5}', () => {
  const t1 = new RRefined('x', rInt, pgt(pvar('x'), pnum(3)));
  const t2 = new RRefined('x', rInt, pgt(pvar('x'), pnum(5)));
  assert.ok(!isSubtype(t1, t2).isSubtype);
});

test('{x:Int | x > 5 && x < 10} <: {x:Int | x > 5} (conjunction implies conjunct)', () => {
  const t1 = new RRefined('x', rInt, pand(pgt(pvar('x'), pnum(5)), plt(pvar('x'), pnum(10))));
  const t2 = new RRefined('x', rInt, pgt(pvar('x'), pnum(5)));
  assert.ok(isSubtype(t1, t2).isSubtype);
});

// ============================================================
// Function subtyping
// ============================================================

test('(posInt → Int) <: (natType → Int) (contravariant param)', () => {
  const f1 = new RFun('x', posInt(), rInt);
  const f2 = new RFun('x', natType(), rInt);
  // f1 takes posInt, f2 takes natType. For f1 <: f2, need natType <: posInt (contravariant)
  // natType !<: posInt, so this should fail
  assert.ok(!isSubtype(f1, f2).isSubtype);
});

test('(natType → Int) <: (posInt → Int) (contravariant)', () => {
  const f1 = new RFun('x', natType(), rInt);
  const f2 = new RFun('x', posInt(), rInt);
  // Need posInt <: natType (contravariant) — yes, x>0 ⇒ x>=0
  assert.ok(isSubtype(f1, f2).isSubtype);
});

test('(Int → posInt) <: (Int → natType) (covariant return)', () => {
  const f1 = new RFun('x', rInt, posInt());
  const f2 = new RFun('x', rInt, natType());
  assert.ok(isSubtype(f1, f2).isSubtype);
});

// ============================================================
// Predicate evaluation
// ============================================================

test('eval: x > 5 with x=10', () => {
  assert.equal(evalPredicate(pgt(pvar('x'), pnum(5)), { x: 10 }), true);
});

test('eval: x > 5 with x=3', () => {
  assert.equal(evalPredicate(pgt(pvar('x'), pnum(5)), { x: 3 }), false);
});

test('eval: x % 2 == 0 with x=4', () => {
  assert.equal(evalPredicate(peq(pmod(pvar('x'), pnum(2)), pnum(0)), { x: 4 }), true);
});

test('eval: x > 0 && x < 100 with x=50', () => {
  assert.equal(evalPredicate(pand(pgt(pvar('x'), pnum(0)), plt(pvar('x'), pnum(100))), { x: 50 }), true);
});

// ============================================================
// Report
// ============================================================

console.log(`\nRefinement type tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
