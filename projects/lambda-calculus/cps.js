/**
 * CPS Transformation — Continuation-Passing Style
 * 
 * Transforms lambda calculus terms into CPS where every function
 * takes an explicit continuation argument. This makes:
 * - Control flow explicit (every call is a tail call)
 * - Evaluation order fixed
 * - No implicit stack needed
 * 
 * Also implements:
 * - A-Normal Form (ANF) transformation
 * - CPS → direct style back-translation
 * - Optimization passes (administrative beta reduction)
 */

import { Var, Abs, App, parse, reduce, freeVars } from './lambda.js';

// ============================================================
// Fresh Variable Generation
// ============================================================

let cpsCounter = 0;
function freshK() { return `k${cpsCounter++}`; }
function freshV() { return `v${cpsCounter++}`; }
function resetCPS() { cpsCounter = 0; }

// ============================================================
// CPS Transformation (Fischer/Plotkin style)
// 
// For call-by-value:
// [[x]]     k = k x
// [[λx.M]]  k = k (λx.λk'. [[M]] k')
// [[M N]]   k = [[M]] (λm. [[N]] (λn. m n k))
// ============================================================

function cpsTransform(expr) {
  resetCPS();
  const kName = freshK();
  const body = cpsTransformInner(expr, new Var(kName));
  return new Abs(kName, body);
}

function cpsTransformInner(expr, cont) {
  // Variable: k x
  if (expr instanceof Var) {
    return new App(cont, expr);
  }
  
  // Abstraction: k (λx.λk'. [[body]] k')
  if (expr instanceof Abs) {
    const kPrime = freshK();
    const cpsBody = cpsTransformInner(expr.body, new Var(kPrime));
    const cpsFn = new Abs(expr.param, new Abs(kPrime, cpsBody));
    return new App(cont, cpsFn);
  }
  
  // Application: [[M]] (λm. [[N]] (λn. m n k))
  if (expr instanceof App) {
    const mVar = freshV();
    const nVar = freshV();
    
    const innerApply = new App(new App(new Var(mVar), new Var(nVar)), cont);
    const nCont = new Abs(nVar, innerApply);
    const mBody = cpsTransformInner(expr.arg, nCont);
    const mCont = new Abs(mVar, mBody);
    return cpsTransformInner(expr.func, mCont);
  }
  
  throw new Error(`Unknown expression in CPS: ${expr}`);
}

// ============================================================
// One-pass CPS (Danvy & Filinski style)
// More efficient: reduces administrative redexes during transformation
// ============================================================

function cpsOnePass(expr) {
  resetCPS();
  const kName = freshK();
  const body = cpsOnePassInner(expr, x => new App(new Var(kName), x));
  return new Abs(kName, body);
}

function cpsOnePassInner(expr, metaCont) {
  // Variable: trivial — pass directly to meta-continuation
  if (expr instanceof Var) {
    return metaCont(expr);
  }
  
  // Abstraction: trivial — wrap in CPS-lambda and pass
  if (expr instanceof Abs) {
    const kPrime = freshK();
    const cpsBody = cpsOnePassInner(expr.body, x => new App(new Var(kPrime), x));
    const cpsFn = new Abs(expr.param, new Abs(kPrime, cpsBody));
    return metaCont(cpsFn);
  }
  
  // Application: evaluate func, then arg, then apply
  if (expr instanceof App) {
    return cpsOnePassInner(expr.func, f => {
      return cpsOnePassInner(expr.arg, a => {
        const kName = freshK();
        const kVar = new Var(kName);
        const innerBody = metaCont(kVar);
        return new App(new App(f, a), new Abs(kName, innerBody));
      });
    });
  }
  
  throw new Error(`Unknown expression in one-pass CPS: ${expr}`);
}

// ============================================================
// A-Normal Form (ANF)
// Like CPS but without continuations — just names all intermediate values
// ============================================================

function anfTransform(expr) {
  resetCPS();
  return anfInner(expr, x => x);
}

function anfInner(expr, ctx) {
  // Variable: trivial
  if (expr instanceof Var) {
    return ctx(expr);
  }
  
  // Abstraction: normalize body
  if (expr instanceof Abs) {
    return ctx(new Abs(expr.param, anfInner(expr.body, x => x)));
  }
  
  // Application: name both func and arg, then apply
  if (expr instanceof App) {
    return anfInner(expr.func, f => {
      return anfInner(expr.arg, a => {
        // If f is a value (var or lambda), don't need to name it
        if (f instanceof Var || f instanceof Abs) {
          if (a instanceof Var || a instanceof Abs) {
            return ctx(new App(f, a));
          }
          // Name the argument
          const aName = freshV();
          return makeLet(aName, a, ctx(new App(f, new Var(aName))));
        }
        // Name the function
        const fName = freshV();
        if (a instanceof Var || a instanceof Abs) {
          return makeLet(fName, f, ctx(new App(new Var(fName), a)));
        }
        // Name both
        const aName = freshV();
        return makeLet(fName, f, makeLet(aName, a, ctx(new App(new Var(fName), new Var(aName)))));
      });
    });
  }
  
  throw new Error(`Unknown expression in ANF: ${expr}`);
}

// Let-binding encoded as (λname.body) value
function makeLet(name, value, body) {
  return new App(new Abs(name, body), value);
}

// ============================================================
// Administrative Beta Reduction
// Reduces (λk. k V) to V when safe
// ============================================================

function adminReduce(expr) {
  return reduce(expr, 'normal', 5000).result;
}

// ============================================================
// CPS Evaluation Helper
// To evaluate a CPS term, apply it to the identity continuation
// ============================================================

function evalCPS(cpsExpr) {
  const id = parse('λx.x');
  return reduce(new App(cpsExpr, id), 'normal', 10000);
}

// ============================================================
// Size metric
// ============================================================

function termSize(expr) {
  if (expr instanceof Var) return 1;
  if (expr instanceof Abs) return 1 + termSize(expr.body);
  if (expr instanceof App) return 1 + termSize(expr.func) + termSize(expr.arg);
  return 0;
}

// ============================================================
// Exports
// ============================================================

export {
  cpsTransform, cpsOnePass,
  anfTransform,
  adminReduce, evalCPS,
  termSize, resetCPS,
  makeLet,
};
