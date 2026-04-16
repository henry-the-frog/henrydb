/**
 * Let Floating: Move let bindings outward for optimization
 * 
 * let-float-out: move lets to wider scope (share more, reduce allocation)
 * let-float-in: move lets to narrower scope (reduce lifetime)
 * 
 * GHC uses both passes in its simplifier.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } toString() { return `(let ${this.var} = ${this.init} in ${this.body})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }

function freeVars(expr) {
  switch (expr.tag) {
    case 'Var': return new Set([expr.name]);
    case 'Num': return new Set();
    case 'Lam': { const fv = freeVars(expr.body); fv.delete(expr.var); return fv; }
    case 'App': return new Set([...freeVars(expr.fn), ...freeVars(expr.arg)]);
    case 'Let': { const fv = new Set([...freeVars(expr.init), ...freeVars(expr.body)]); fv.delete(expr.var); return fv; }
  }
}

// Float out: move lets out of lambdas (when safe)
function floatOut(expr) {
  switch (expr.tag) {
    case 'Var': case 'Num': return expr;
    case 'Lam': {
      const body = floatOut(expr.body);
      // If body is a let whose init doesn't use the lambda var, float it out
      if (body.tag === 'Let' && !freeVars(body.init).has(expr.var)) {
        return new Let(body.var, body.init, new Lam(expr.var, floatOut(body.body)));
      }
      return new Lam(expr.var, body);
    }
    case 'App': return new App(floatOut(expr.fn), floatOut(expr.arg));
    case 'Let': return new Let(expr.var, floatOut(expr.init), floatOut(expr.body));
  }
}

// Float in: move lets closer to use sites
function floatIn(expr) {
  if (expr.tag !== 'Let') {
    switch (expr.tag) {
      case 'Lam': return new Lam(expr.var, floatIn(expr.body));
      case 'App': return new App(floatIn(expr.fn), floatIn(expr.arg));
      default: return expr;
    }
  }
  
  const { var: v, init, body } = expr;
  const fBody = floatIn(body);
  
  // If only used in fn or arg of App, float into that branch
  if (fBody.tag === 'App') {
    const inFn = freeVars(fBody.fn).has(v);
    const inArg = freeVars(fBody.arg).has(v);
    if (inFn && !inArg) return new App(new Let(v, init, fBody.fn), fBody.arg);
    if (!inFn && inArg) return new App(fBody.fn, new Let(v, init, fBody.arg));
  }
  
  // If only in then/else of a conditional, float there
  return new Let(v, floatIn(init), fBody);
}

// Count lets at each depth
function letDepth(expr, depth = 0, result = []) {
  switch (expr.tag) {
    case 'Let': result.push({ var: expr.var, depth }); letDepth(expr.init, depth, result); letDepth(expr.body, depth, result); break;
    case 'Lam': letDepth(expr.body, depth + 1, result); break;
    case 'App': letDepth(expr.fn, depth, result); letDepth(expr.arg, depth, result); break;
  }
  return result;
}

export { Var, Lam, App, Let, Num, freeVars, floatOut, floatIn, letDepth };
