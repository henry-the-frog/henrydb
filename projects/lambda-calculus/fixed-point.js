/**
 * Fixed-Point Combinators: Y, Z, Θ
 * 
 * Y = λf.(λx.f(x x))(λx.f(x x))  — Curry's Y (CBN only)
 * Z = λf.(λx.f(λv.x x v))(λx.f(λv.x x v))  — strict Y (works in CBV)
 * Θ = (λx.λf.f(x x f))(λx.λf.f(x x f))  — Turing's fixed-point combinator
 */

// Z combinator (call-by-value Y)
const Z = f => (x => f(v => x(x)(v)))(x => f(v => x(x)(v)));

// Direct recursion via Z
const factorial = Z(f => n => n === 0 ? 1 : n * f(n - 1));
const fibonacci = Z(f => n => n <= 1 ? n : f(n - 1) + f(n - 2));
function gcd(a, b) { return b === 0 ? a : gcd(b, a % b); }
function ackermann(m, n) {
  if (m === 0) return n + 1;
  if (n === 0) return ackermann(m - 1, 1);
  return ackermann(m - 1, ackermann(m, n - 1));
}

// Church-style Y (only for reference — diverges in strict languages)
// const Y = f => (x => f(x(x)))(x => f(x(x)));

// Mutual recursion via product of fixed points
function mutualFix(fns) {
  // fns: array of (self, others) => result
  const results = {};
  for (let i = 0; i < fns.length; i++) {
    results[i] = (...args) => fns[i](results)(...args);
  }
  return results;
}

// Example: isEven/isOdd via mutual recursion
const { 0: isEven, 1: isOdd } = mutualFix([
  self => n => n === 0 ? true : self[1](n - 1),
  self => n => n === 0 ? false : self[0](n - 1)
]);

// Memoized fixed point
function memoFix(f) {
  const cache = new Map();
  const memoized = n => {
    if (cache.has(n)) return cache.get(n);
    const result = f(memoized)(n);
    cache.set(n, result);
    return result;
  };
  return memoized;
}

const memoFib = memoFix(f => n => n <= 1 ? n : f(n - 1) + f(n - 2));

export { Z, factorial, fibonacci, gcd, ackermann, mutualFix, isEven, isOdd, memoFix, memoFib };
