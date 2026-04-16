import { strict as assert } from 'assert';
import {
  zero, one, two, three, four, five, nat,
  tlTrue, tlFalse, tlNil, cons, list,
  tlAdd, tlMul, tlEqual, tlLessThan,
  tlIf, tlNot, tlAnd, tlOr,
  tlLength, tlAppend, tlReverse, tlMap, tlFilter,
  TLBase, TLVec, vec, vecAppend, vecHead, vecTail,
  tlSub, tlMin, tlMax, tlIsEven,
  typeEquals
} from './type-level.js';

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
// Peano naturals
// ============================================================

test('nat(0) = Zero', () => assert.ok(typeEquals(nat(0), zero)));
test('nat(3) = Succ(Succ(Succ(Zero)))', () => assert.ok(typeEquals(nat(3), three)));
test('nat(5).toNumber() = 5', () => assert.equal(nat(5).toNumber(), 5));

// ============================================================
// Arithmetic
// ============================================================

test('Add(2, 3) = 5', () => assert.ok(typeEquals(tlAdd(two, three), five)));
test('Add(0, n) = n', () => assert.ok(typeEquals(tlAdd(zero, four), four)));
test('Add(n, 0) = n', () => assert.ok(typeEquals(tlAdd(three, zero), three)));

test('Mul(2, 3) = 6', () => assert.ok(typeEquals(tlMul(two, three), nat(6))));
test('Mul(0, 5) = 0', () => assert.ok(typeEquals(tlMul(zero, five), zero)));
test('Mul(1, n) = n', () => assert.ok(typeEquals(tlMul(one, four), four)));

test('Sub(5, 3) = 2', () => assert.ok(typeEquals(tlSub(five, three), two)));
test('Sub(3, 5) = 0 (saturating)', () => assert.ok(typeEquals(tlSub(three, five), zero)));
test('Sub(n, 0) = n', () => assert.ok(typeEquals(tlSub(four, zero), four)));

// ============================================================
// Comparison
// ============================================================

test('Equal(3, 3) = True', () => assert.ok(typeEquals(tlEqual(three, three), tlTrue)));
test('Equal(2, 3) = False', () => assert.ok(typeEquals(tlEqual(two, three), tlFalse)));
test('LessThan(2, 3) = True', () => assert.ok(typeEquals(tlLessThan(two, three), tlTrue)));
test('LessThan(3, 2) = False', () => assert.ok(typeEquals(tlLessThan(three, two), tlFalse)));
test('LessThan(0, 1) = True', () => assert.ok(typeEquals(tlLessThan(zero, one), tlTrue)));
test('LessThan(0, 0) = False', () => assert.ok(typeEquals(tlLessThan(zero, zero), tlFalse)));

// ============================================================
// Boolean logic
// ============================================================

test('Not(True) = False', () => assert.ok(typeEquals(tlNot(tlTrue), tlFalse)));
test('Not(False) = True', () => assert.ok(typeEquals(tlNot(tlFalse), tlTrue)));
test('And(True, True) = True', () => assert.ok(typeEquals(tlAnd(tlTrue, tlTrue), tlTrue)));
test('And(True, False) = False', () => assert.ok(typeEquals(tlAnd(tlTrue, tlFalse), tlFalse)));
test('Or(False, True) = True', () => assert.ok(typeEquals(tlOr(tlFalse, tlTrue), tlTrue)));
test('Or(False, False) = False', () => assert.ok(typeEquals(tlOr(tlFalse, tlFalse), tlFalse)));

// ============================================================
// Conditionals
// ============================================================

test('If(True, 1, 2) = 1', () => assert.ok(typeEquals(tlIf(tlTrue, one, two), one)));
test('If(False, 1, 2) = 2', () => assert.ok(typeEquals(tlIf(tlFalse, one, two), two)));

// ============================================================
// Min/Max
// ============================================================

test('Min(2, 5) = 2', () => assert.ok(typeEquals(tlMin(two, five), two)));
test('Max(2, 5) = 5', () => assert.ok(typeEquals(tlMax(two, five), five)));
test('Min(3, 3) = 3', () => assert.ok(typeEquals(tlMin(three, three), three)));

// ============================================================
// IsEven
// ============================================================

test('IsEven(0) = True', () => assert.ok(typeEquals(tlIsEven(zero), tlTrue)));
test('IsEven(1) = False', () => assert.ok(typeEquals(tlIsEven(one), tlFalse)));
test('IsEven(4) = True', () => assert.ok(typeEquals(tlIsEven(four), tlTrue)));
test('IsEven(5) = False', () => assert.ok(typeEquals(tlIsEven(five), tlFalse)));

// ============================================================
// Type-level lists
// ============================================================

test('Length(Nil) = 0', () => assert.ok(typeEquals(tlLength(tlNil), zero)));
test('Length([1,2,3]) = 3', () => assert.ok(typeEquals(tlLength(list(one, two, three)), three)));

test('Append(Nil, [1]) = [1]', () => {
  const result = tlAppend(tlNil, list(one));
  assert.ok(typeEquals(result, list(one)));
});

test('Append([1,2], [3]) = [1,2,3]', () => {
  const result = tlAppend(list(one, two), list(three));
  assert.ok(typeEquals(result, list(one, two, three)));
});

test('Reverse([1,2,3]) = [3,2,1]', () => {
  const result = tlReverse(list(one, two, three));
  assert.ok(typeEquals(result, list(three, two, one)));
});

test('Map Succ over [1,2,3] = [2,3,4]', () => {
  const succ = n => new (n.constructor === zero.constructor ? n.__proto__.constructor : n.constructor)(n);
  // Actually just use TLSucc
  const result = tlMap(n => ({ tag: 'TLSucc', pred: n, toString() { return `Succ(${n})`; }, toNumber() { return 1 + n.toNumber(); } }), list(one, two, three));
  assert.equal(result.head.toNumber(), 2);
  assert.equal(result.tail.head.toNumber(), 3);
  assert.equal(result.tail.tail.head.toNumber(), 4);
});

test('Filter even from [1,2,3,4] = [2,4]', () => {
  const result = tlFilter(tlIsEven, list(one, two, three, four));
  assert.ok(typeEquals(result.head, two));
  assert.ok(typeEquals(result.tail.head, four));
  assert.ok(typeEquals(result.tail.tail, tlNil));
});

// ============================================================
// Vec (length-indexed lists)
// ============================================================

const tInt = new TLBase('Int');
const tStr = new TLBase('String');

test('Vec(Int, 3)', () => {
  const v = new TLVec(tInt, three);
  assert.equal(v.toString(), 'Vec(Int, Succ(Succ(Succ(Zero))))');
});

test('vecAppend: Vec(Int,2) ++ Vec(Int,3) = Vec(Int,5)', () => {
  const v1 = new TLVec(tInt, two);
  const v2 = new TLVec(tInt, three);
  const result = vecAppend(v1, v2);
  assert.ok(typeEquals(result.length, five));
});

test('vecHead: Vec(Int, Succ(n)) returns Int', () => {
  const v = new TLVec(tInt, three);
  const headType = vecHead(v);
  assert.ok(typeEquals(headType, tInt));
});

test('vecHead: Vec(_, Zero) throws type error', () => {
  const v = new TLVec(tInt, zero);
  assert.throws(() => vecHead(v), /cannot take head.*Zero/i);
});

test('vecTail: Vec(Int, 3) = Vec(Int, 2)', () => {
  const v = new TLVec(tInt, three);
  const tail = vecTail(v);
  assert.ok(typeEquals(tail.length, two));
});

test('vecTail: Vec(_, Zero) throws type error', () => {
  const v = new TLVec(tInt, zero);
  assert.throws(() => vecTail(v), /cannot take tail.*Zero/i);
});

test('vecAppend type mismatch throws', () => {
  const v1 = new TLVec(tInt, two);
  const v2 = new TLVec(tStr, three);
  assert.throws(() => vecAppend(v1, v2), /mismatch/);
});

// ============================================================
// Complex: FizzBuzz at the type level
// ============================================================

test('FizzBuzz type-level: classify 15 as FizzBuzz', () => {
  // Is n divisible by k? Check if n mod k = 0
  // Type-level modulo using repeated subtraction
  function tlMod(n, k) {
    if (n.tag === 'TLZero') return zero;
    const sub = tlSub(n, k);
    if (typeEquals(sub, zero) && !typeEquals(n, zero) && typeEquals(tlLessThan(n, k), tlTrue)) return n;
    if (typeEquals(sub, zero)) return zero;
    return tlMod(sub, k);
  }
  
  const n15 = nat(15);
  assert.ok(typeEquals(tlMod(n15, three), zero)); // 15 % 3 = 0
  assert.ok(typeEquals(tlMod(n15, five), zero));   // 15 % 5 = 0
  assert.ok(!typeEquals(tlMod(nat(7), three), zero)); // 7 % 3 ≠ 0
});

// ============================================================
// Report
// ============================================================

console.log(`\nType-level computation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
