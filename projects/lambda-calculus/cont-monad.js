/**
 * Continuation Monad + callcc
 * 
 * The continuation monad CPS-transforms computations:
 *   Cont r a = (a → r) → r
 * 
 * callcc captures the current continuation, enabling:
 * - Early return
 * - Exception-like behavior
 * - Coroutine patterns
 */

// Cont r a = (a → r) → r
class Cont {
  constructor(run) { this.run = run; } // run :: (a → r) → r
}

// return :: a → Cont r a
function creturn(value) {
  return new Cont(k => k(value));
}

// bind :: Cont r a → (a → Cont r b) → Cont r b
function cbind(ma, fn) {
  return new Cont(k => ma.run(a => fn(a).run(k)));
}

// callcc :: ((a → Cont r b) → Cont r a) → Cont r a
function callcc(fn) {
  return new Cont(k => {
    const escape = a => new Cont(_ => k(a)); // Ignore the rest, jump to k
    return fn(escape).run(k);
  });
}

// runCont :: Cont r r → r
function runCont(cont) {
  return cont.run(x => x);
}

// Convenience: chain operations
function chain(...operations) {
  return operations.reduce((acc, op) => cbind(acc, op));
}

// ============================================================
// Examples using callcc
// ============================================================

// Early return from a computation
function earlyReturn(n) {
  return callcc(exit => {
    if (n < 0) return exit('negative!');
    return creturn(n * 2);
  });
}

// Exception-like: try/catch
function tryCatch(tryFn, catchFn) {
  return callcc(handler => {
    const throwErr = err => handler(catchFn(err));
    return tryFn(throwErr);
  });
}

export { Cont, creturn, cbind, callcc, runCont, chain, earlyReturn, tryCatch };
