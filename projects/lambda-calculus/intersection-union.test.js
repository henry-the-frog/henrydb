import { strict as assert } from 'assert';
import {
  TBase, TIntersection, TUnion, TFun, TRecord, TTop, TBottom,
  tInt, tBool, tStr, tTop, tBottom,
  isSubtype, typeEquals, simplify, narrow, widen
} from './intersection-union.js';

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
// Subtyping: basics
// ============================================================

test('Int <: Int', () => assert.ok(isSubtype(tInt, tInt)));
test('Int !<: Bool', () => assert.ok(!isSubtype(tInt, tBool)));
test('T <: ⊤', () => assert.ok(isSubtype(tInt, tTop)));
test('⊥ <: T', () => assert.ok(isSubtype(tBottom, tInt)));
test('⊤ !<: Int', () => assert.ok(!isSubtype(tTop, tInt)));
test('Int !<: ⊥', () => assert.ok(!isSubtype(tInt, tBottom)));

// ============================================================
// Intersection subtyping
// ============================================================

test('Int & Bool <: Int', () => assert.ok(isSubtype(new TIntersection(tInt, tBool), tInt)));
test('Int & Bool <: Bool', () => assert.ok(isSubtype(new TIntersection(tInt, tBool), tBool)));
test('Int <: Int & Int', () => assert.ok(isSubtype(tInt, new TIntersection(tInt, tInt))));
test('Int !<: Int & Bool', () => assert.ok(!isSubtype(tInt, new TIntersection(tInt, tBool))));

// ============================================================
// Union subtyping
// ============================================================

test('Int <: Int | Bool', () => assert.ok(isSubtype(tInt, new TUnion(tInt, tBool))));
test('Bool <: Int | Bool', () => assert.ok(isSubtype(tBool, new TUnion(tInt, tBool))));
test('Int | Bool <: Int | Bool | Str', () => {
  const t1 = new TUnion(tInt, tBool);
  const t2 = new TUnion(new TUnion(tInt, tBool), tStr);
  assert.ok(isSubtype(t1, t2));
});
test('Int | Bool !<: Int', () => assert.ok(!isSubtype(new TUnion(tInt, tBool), tInt)));

// ============================================================
// Function subtyping
// ============================================================

test('(Int → Bool) <: (Int → Bool)', () => {
  assert.ok(isSubtype(new TFun(tInt, tBool), new TFun(tInt, tBool)));
});

test('(⊤ → Int) <: (Int → Int) (contravariant param)', () => {
  // Wider param type is a subtype (can accept more inputs)
  assert.ok(isSubtype(new TFun(tTop, tInt), new TFun(tInt, tInt)));
});

test('(Int → ⊥) !<: (Int → Int) (covariant return)', () => {
  // ⊥ <: Int, so this should be valid
  assert.ok(isSubtype(new TFun(tInt, tBottom), new TFun(tInt, tInt)));
});

// ============================================================
// Record subtyping
// ============================================================

test('{name: Str, age: Int} <: {name: Str}', () => {
  const t1 = new TRecord(new Map([['name', tStr], ['age', tInt]]));
  const t2 = new TRecord(new Map([['name', tStr]]));
  assert.ok(isSubtype(t1, t2));
});

test('{name: Str} !<: {name: Str, age: Int}', () => {
  const t1 = new TRecord(new Map([['name', tStr]]));
  const t2 = new TRecord(new Map([['name', tStr], ['age', tInt]]));
  assert.ok(!isSubtype(t1, t2));
});

// ============================================================
// Simplification
// ============================================================

test('Int & ⊤ = Int', () => assert.ok(typeEquals(simplify(new TIntersection(tInt, tTop)), tInt)));
test('⊤ & Int = Int', () => assert.ok(typeEquals(simplify(new TIntersection(tTop, tInt)), tInt)));
test('Int & ⊥ = ⊥', () => assert.ok(typeEquals(simplify(new TIntersection(tInt, tBottom)), tBottom)));
test('Int | ⊥ = Int', () => assert.ok(typeEquals(simplify(new TUnion(tInt, tBottom)), tInt)));
test('Int | ⊤ = ⊤', () => assert.ok(typeEquals(simplify(new TUnion(tInt, tTop)), tTop)));
test('Int & Int = Int', () => assert.ok(typeEquals(simplify(new TIntersection(tInt, tInt)), tInt)));
test('Int | Int = Int', () => assert.ok(typeEquals(simplify(new TUnion(tInt, tInt)), tInt)));

// ============================================================
// Narrowing (flow-sensitive)
// ============================================================

test('narrow Int|Str to Int', () => {
  const t = new TUnion(tInt, tStr);
  assert.ok(typeEquals(narrow(t, tInt), tInt));
});

test('narrow Int|Str to Str', () => {
  const t = new TUnion(tInt, tStr);
  assert.ok(typeEquals(narrow(t, tStr), tStr));
});

test('widen Int|Str - Int = Str', () => {
  const t = new TUnion(tInt, tStr);
  assert.ok(typeEquals(widen(t, tInt), tStr));
});

// ============================================================
// Pretty printing
// ============================================================

test('intersection toString', () => {
  assert.equal(new TIntersection(tInt, tBool).toString(), '(Int & Bool)');
});

test('union toString', () => {
  assert.equal(new TUnion(tInt, tStr).toString(), '(Int | Str)');
});

// ============================================================
// Report
// ============================================================

console.log(`\nIntersection/Union type tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
