/**
 * Arity Analysis: Determine the "real" arity of functions
 * 
 * A function might be defined as f = λx.λy.x+y (arity 2)
 * but called as g = f 3 (partial application, arity 1).
 * 
 * Knowing arity helps: avoid building partial application closures,
 * optimize calling convention.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }

// Manifest arity: count nested lambdas
function manifestArity(expr) {
  let count = 0;
  let current = expr;
  while (current.tag === 'Lam') { count++; current = current.body; }
  return count;
}

// Call arity: how many args at each call site
function callArity(expr, fnName) {
  const arities = [];
  function walk(e) {
    if (e.tag === 'App') {
      let args = 0;
      let fn = e;
      while (fn.tag === 'App') { args++; walk(fn.arg); fn = fn.fn; }
      if (fn.tag === 'Var' && fn.name === fnName) arities.push(args);
    }
    if (e.tag === 'Lam') walk(e.body);
    if (e.tag === 'Let') { walk(e.init); walk(e.body); }
  }
  walk(expr);
  return arities;
}

// Minimum call arity: the fewest args any call site provides
function minCallArity(expr, fnName) {
  const arities = callArity(expr, fnName);
  return arities.length === 0 ? 0 : Math.min(...arities);
}

// Eta-expand to match arity
function etaExpand(expr, targetArity) {
  const currentArity = manifestArity(expr);
  if (currentArity >= targetArity) return expr;
  
  // Build params and innermost application
  const params = [];
  for (let i = currentArity; i < targetArity; i++) params.push(`_a${i}`);
  
  let body = expr;
  for (const p of params) body = new App(body, new Var(p));
  
  // Wrap in lambdas (outermost first)
  let result = body;
  for (let i = params.length - 1; i >= 0; i--) result = new Lam(params[i], result);
  return result;
}

// Check if function is always fully applied
function isAlwaysFullyApplied(expr, fnName) {
  const arity = manifestArity(lookupBinding(expr, fnName) || new Num(0));
  const callArities = callArity(expr, fnName);
  return callArities.every(a => a >= arity);
}

function lookupBinding(expr, name) {
  if (expr.tag === 'Let' && expr.var === name) return expr.init;
  if (expr.tag === 'Let') return lookupBinding(expr.body, name) || lookupBinding(expr.init, name);
  if (expr.tag === 'Lam') return lookupBinding(expr.body, name);
  if (expr.tag === 'App') return lookupBinding(expr.fn, name) || lookupBinding(expr.arg, name);
  return null;
}

export { Var, Num, Lam, App, Let, manifestArity, callArity, minCallArity, etaExpand, isAlwaysFullyApplied };
