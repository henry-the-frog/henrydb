/**
 * Scott Encodings: Pattern matching as lambda terms
 * 
 * Unlike Church (fold-based), Scott encoding represents case analysis directly.
 * Each constructor takes one continuation per constructor.
 * 
 * Bool: True = λt.λf.t, False = λt.λf.f  (same as Church)
 * Nat: Zero = λz.λs.z, Succ(n) = λz.λs.s(n)  (s gets the PREDECESSOR, not fold!)
 * List: Nil = λn.λc.n, Cons(h,t) = λn.λc.c(h)(t)
 */

// Scott Booleans (identical to Church)
const sTrue = t => f => t;
const sFalse = t => f => f;
const fromSBool = b => b(true)(false);

// Scott Naturals (differ from Church: succ gives predecessor, not fold)
const sZero = z => s => z;
const sSucc = n => z => s => s(n);

function fromSNat(n) {
  const ZERO_MARKER = Symbol('zero');
  let count = 0;
  let current = n;
  while (true) {
    const result = current(ZERO_MARKER)(pred => pred);
    if (result === ZERO_MARKER) return count;
    count++;
    current = result;
  }
}

function toSNat(num) {
  let n = sZero;
  for (let i = 0; i < num; i++) n = sSucc(n);
  return n;
}

// Recursive operations on Scott naturals
function sAdd(m, n) {
  return m(n)(pred => sSucc(sAdd(pred, n)));
}

function sIsZero(n) { return n(sTrue)(pred => sFalse); }

// Scott Lists
const sNil = n => c => n;
const sCons = h => t => n => c => c(h)(t);

function fromSList(lst) {
  const result = [];
  let current = lst;
  while (true) {
    const r = current(null)(h => t => ({ head: h, tail: t }));
    if (r === null) return result;
    result.push(r.head);
    current = r.tail;
  }
}

function toSList(arr) {
  let lst = sNil;
  for (let i = arr.length - 1; i >= 0; i--) lst = sCons(arr[i])(lst);
  return lst;
}

function sHead(lst) { return lst(undefined)(h => t => h); }
function sTail(lst) { return lst(sNil)(h => t => t); }
function sLength(lst) {
  let count = 0;
  let current = lst;
  while (true) {
    const r = current(null)(h => t => t);
    if (r === null) return count;
    count++;
    current = r;
  }
}

// Scott Maybe
const sNothing = n => j => n;
const sJust = v => n => j => j(v);
function fromSMaybe(m) { return m({ tag: 'Nothing' })(v => ({ tag: 'Just', value: v })); }

// Scott Either
const sLeft = v => l => r => l(v);
const sRight = v => l => r => r(v);
function fromSEither(e) { return e(v => ({ tag: 'Left', value: v }))(v => ({ tag: 'Right', value: v })); }

export {
  sTrue, sFalse, fromSBool,
  sZero, sSucc, fromSNat, toSNat, sAdd, sIsZero,
  sNil, sCons, fromSList, toSList, sHead, sTail, sLength,
  sNothing, sJust, fromSMaybe,
  sLeft, sRight, fromSEither
};
