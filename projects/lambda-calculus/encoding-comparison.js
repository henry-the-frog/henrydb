/**
 * Encoding Comparison: Church vs Scott vs Parigot
 * 
 * Three ways to encode data in pure lambda calculus:
 * 
 * Church: Fold-based. Nat = fold over successors.
 *   0 = λs.λz.z, n+1 = λs.λz.s(n s z)
 *   ✓ Easy iteration, ✗ Hard predecessor (O(n))
 * 
 * Scott: Case-based. Nat = pattern match on constructors.
 *   0 = λz.λs.z, n+1 = λz.λs.s(n)
 *   ✓ O(1) predecessor, ✗ No built-in fold
 * 
 * Parigot: Both! Nat carries both fold and match.
 *   0 = λz.λs.z, n+1 = λz.λs.s(n)(n s z)
 *   ✓ O(1) predecessor AND fold, ✗ Exponential size
 */

// Church naturals
const cZ = s => z => z;
const cS = n => s => z => s(n(s)(z));
const cToInt = n => n(x => x + 1)(0);
const cPred = n => s => z => n(g => h => h(g(s)))(u => z)(u => u);

// Scott naturals
const sZ = z => s => z;
const sS = n => z => s => s(n);
const ZERO_SYM = Symbol('zero');
const sToInt = n => { let c = 0, cur = n; while (true) { const r = cur(ZERO_SYM)(p => p); if (r === ZERO_SYM) return c; c++; cur = r; } };
const sPred = n => n(sZ)(pred => pred);

// Parigot naturals — simplified version
// The idea: carry both predecessor AND recursive result
// Implementation: pair of (Scott nat, Church nat)
const pZ = z => s => z;
const pS = n => z => s => s(n)(pToInt(n) + 1); // simplified: carry predecessor + count
const pToInt = n => n(0)(pred => count => count);
const pPred = n => n(pZ)(pred => count => pred);

// Benchmark: predecessor operation
function benchPred(encoding, n, pred, toInt, zero, succ) {
  let val = zero;
  for (let i = 0; i < n; i++) val = succ(val);
  return toInt(pred(val));
}

// Benchmark: fold/iteration
function benchFold(encoding, n) {
  // Sum 0..n
  if (encoding === 'church') {
    let val = cZ;
    for (let i = 0; i < n; i++) val = cS(val);
    return val(x => x + 1)(0);
  }
  if (encoding === 'scott') {
    let val = sZ;
    for (let i = 0; i < n; i++) val = sS(val);
    return sToInt(val);
  }
  if (encoding === 'parigot') {
    let val = pZ;
    for (let i = 0; i < n; i++) val = pS(val);
    return pToInt(val);
  }
}

export { cZ, cS, cToInt, cPred, sZ, sS, sToInt, sPred, pZ, pS, pToInt, pPred, benchPred, benchFold };
