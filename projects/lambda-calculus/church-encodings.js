/**
 * Church Encodings: Data as pure lambda terms
 * 
 * Represent ALL data using only functions:
 * - Bool: λt.λf.t (true), λt.λf.f (false)
 * - Nat: λs.λz.z (0), λs.λz.s(z) (1), λs.λz.s(s(z)) (2), ...
 * - Pair: λf.f a b
 * - List: fold as representation
 */

// Church Booleans
const cTrue = t => f => t;
const cFalse = t => f => f;
const cNot = b => b(cFalse)(cTrue);
const cAnd = a => b => a(b)(cFalse);
const cOr = a => b => a(cTrue)(b);
const cIf = b => t => f => b(t)(f);

function fromCBool(b) { return b(true)(false); }
function toCBool(v) { return v ? cTrue : cFalse; }

// Church Naturals
const cZero = s => z => z;
const cSucc = n => s => z => s(n(s)(z));
const cAdd = m => n => s => z => m(s)(n(s)(z));
const cMul = m => n => s => m(n(s));
const cPow = m => n => n(m);
const cPred = n => s => z => n(g => h => h(g(s)))(u => z)(u => u);
const cIsZero = n => n(x => cFalse)(cTrue);

function fromCNat(n) { return n(x => x + 1)(0); }
function toCNat(num) { let n = cZero; for (let i = 0; i < num; i++) n = cSucc(n); return n; }

// Church Pairs
const cPair = a => b => f => f(a)(b);
const cFst = p => p(a => b => a);
const cSnd = p => p(a => b => b);

// Church Lists
const cNil = c => n => n;
const cCons = h => t => c => n => c(h)(t(c)(n));

function fromCList(lst) { return lst((h) => (t) => [h, ...t])([]); }
function toCList(arr) { let lst = cNil; for (let i = arr.length - 1; i >= 0; i--) lst = cCons(arr[i])(lst); return lst; }

const cHead = lst => lst((h) => (t) => h)(undefined);
const cLength = lst => lst(h => t => t + 1)(0);
const cMap = f => lst => c => n => lst((h) => (t) => c(f(h))(t))(n);
const cFold = lst => lst;

// Church Maybe
const cNothing = s => n => n;
const cJust = v => s => n => s(v);
function fromCMaybe(m) { return m(v => ({ tag: 'Just', value: v }))({ tag: 'Nothing' }); }

export {
  cTrue, cFalse, cNot, cAnd, cOr, cIf, fromCBool, toCBool,
  cZero, cSucc, cAdd, cMul, cPow, cPred, cIsZero, fromCNat, toCNat,
  cPair, cFst, cSnd,
  cNil, cCons, fromCList, toCList, cHead, cLength, cMap, cFold,
  cNothing, cJust, fromCMaybe
};
