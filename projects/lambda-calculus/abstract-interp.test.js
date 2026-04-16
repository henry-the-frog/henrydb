import { strict as assert } from 'assert';
import {
  Sign, SignDomain, Interval, IntervalDomain, Const, ConstDomain,
  AbstractInterpreter,
  num, vr, add, sub, mul, div, let_
} from './abstract-interp.js';

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
// Sign Domain
// ============================================================

test('sign: pos + pos = pos', () => assert.equal(SignDomain.add(Sign.POS, Sign.POS), Sign.POS));
test('sign: neg + neg = neg', () => assert.equal(SignDomain.add(Sign.NEG, Sign.NEG), Sign.NEG));
test('sign: pos + neg = top', () => assert.equal(SignDomain.add(Sign.POS, Sign.NEG), Sign.TOP));
test('sign: pos * neg = neg', () => assert.equal(SignDomain.mul(Sign.POS, Sign.NEG), Sign.NEG));
test('sign: neg * neg = pos', () => assert.equal(SignDomain.mul(Sign.NEG, Sign.NEG), Sign.POS));
test('sign: zero * anything = zero', () => assert.equal(SignDomain.mul(Sign.ZERO, Sign.POS), Sign.ZERO));
test('sign: div by zero = bot', () => assert.equal(SignDomain.div(Sign.POS, Sign.ZERO), Sign.BOT));
test('sign: join pos neg = top', () => assert.equal(SignDomain.join(Sign.POS, Sign.NEG), Sign.TOP));

// ============================================================
// Interval Domain
// ============================================================

test('interval: [1,3] + [2,4] = [3,7]', () => {
  const r = IntervalDomain.add(new Interval(1, 3), new Interval(2, 4));
  assert.equal(r.lo, 3); assert.equal(r.hi, 7);
});

test('interval: [1,5] - [2,3] = [-2,3]', () => {
  const r = IntervalDomain.sub(new Interval(1, 5), new Interval(2, 3));
  assert.equal(r.lo, -2); assert.equal(r.hi, 3);
});

test('interval: [-2,3] * [1,4] = [-8,12]', () => {
  const r = IntervalDomain.mul(new Interval(-2, 3), new Interval(1, 4));
  assert.equal(r.lo, -8); assert.equal(r.hi, 12);
});

test('interval: join [1,3] [5,7] = [1,7]', () => {
  const r = IntervalDomain.join(new Interval(1, 3), new Interval(5, 7));
  assert.equal(r.lo, 1); assert.equal(r.hi, 7);
});

test('interval: meet [1,5] [3,7] = [3,5]', () => {
  const r = IntervalDomain.meet(new Interval(1, 5), new Interval(3, 7));
  assert.equal(r.lo, 3); assert.equal(r.hi, 5);
});

test('interval: exact(5) contains 5', () => {
  assert.ok(Interval.exact(5).contains(5));
  assert.ok(!Interval.exact(5).contains(6));
});

test('interval: widening', () => {
  const old = new Interval(0, 10);
  const new_ = new Interval(0, 15);
  const widened = IntervalDomain.widen(old, new_);
  assert.equal(widened.lo, 0);
  assert.equal(widened.hi, Infinity);
});

// ============================================================
// Constant Domain
// ============================================================

test('const: val(5) + val(3) = val(8)', () => {
  const r = ConstDomain.add(Const.val(5), Const.val(3));
  assert.equal(r.value, 8);
});

test('const: val(5) + top = top', () => {
  const r = ConstDomain.add(Const.val(5), Const.TOP);
  assert.equal(r.tag, 'top');
});

test('const: 0 * top = val(0)', () => {
  const r = ConstDomain.mul(Const.val(0), Const.TOP);
  assert.equal(r.value, 0);
});

test('const: join val(5) val(5) = val(5)', () => {
  const r = ConstDomain.join(Const.val(5), Const.val(5));
  assert.equal(r.value, 5);
});

test('const: join val(5) val(3) = top', () => {
  const r = ConstDomain.join(Const.val(5), Const.val(3));
  assert.equal(r.tag, 'top');
});

// ============================================================
// Abstract Interpreter
// ============================================================

test('abstract interp sign: 3 + (-2) = pos + neg = top', () => {
  const ai = new AbstractInterpreter(SignDomain);
  const result = ai.eval(add(num(3), num(-2)));
  assert.equal(result, Sign.TOP); // Could be pos, neg, or zero
});

test('abstract interp sign: 3 * 4 = pos * pos = pos', () => {
  const ai = new AbstractInterpreter(SignDomain);
  assert.equal(ai.eval(mul(num(3), num(4))), Sign.POS);
});

test('abstract interp interval: let x = 5 in x + 3 = [8,8]', () => {
  const ai = new AbstractInterpreter(IntervalDomain);
  const result = ai.eval(let_('x', num(5), add(vr('x'), num(3))));
  assert.equal(result.lo, 8);
  assert.equal(result.hi, 8);
});

test('abstract interp constant: let x = 5 in let y = x * 2 in y - 3 = const(7)', () => {
  const ai = new AbstractInterpreter(ConstDomain);
  const result = ai.eval(let_('x', num(5), let_('y', mul(vr('x'), num(2)), sub(vr('y'), num(3)))));
  assert.equal(result.value, 7);
});

// ============================================================
// Report
// ============================================================

console.log(`\nAbstract interpretation tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
