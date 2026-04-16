/**
 * Reduction Strategies for Lambda Calculus
 * 
 * - Normal order: reduce leftmost-outermost redex first (finds normal form if it exists)
 * - Applicative order: reduce leftmost-innermost first (call-by-value)
 * - Call-by-need: like normal order but with sharing (Haskell)
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }

let freshCounter = 0;
function fresh(base = 'x') { return `${base}'${freshCounter++}`; }
function resetFresh() { freshCounter = 0; }

function freeVars(expr) {
  switch (expr.tag) {
    case 'Var': return new Set([expr.name]);
    case 'Lam': { const fv = freeVars(expr.body); fv.delete(expr.var); return fv; }
    case 'App': return new Set([...freeVars(expr.fn), ...freeVars(expr.arg)]);
  }
}

function subst(expr, name, replacement) {
  switch (expr.tag) {
    case 'Var': return expr.name === name ? replacement : expr;
    case 'Lam':
      if (expr.var === name) return expr;
      if (freeVars(replacement).has(expr.var)) {
        const newVar = fresh(expr.var);
        const body = subst(expr.body, expr.var, new Var(newVar));
        return new Lam(newVar, subst(body, name, replacement));
      }
      return new Lam(expr.var, subst(expr.body, name, replacement));
    case 'App': return new App(subst(expr.fn, name, replacement), subst(expr.arg, name, replacement));
  }
}

function isRedex(expr) { return expr.tag === 'App' && expr.fn.tag === 'Lam'; }

// Normal order: leftmost-outermost
function normalStep(expr) {
  if (isRedex(expr)) return subst(expr.fn.body, expr.fn.var, expr.arg);
  if (expr.tag === 'App') {
    const fn = normalStep(expr.fn);
    if (fn !== expr.fn) return new App(fn, expr.arg);
    const arg = normalStep(expr.arg);
    if (arg !== expr.arg) return new App(expr.fn, arg);
  }
  if (expr.tag === 'Lam') {
    const body = normalStep(expr.body);
    if (body !== expr.body) return new Lam(expr.var, body);
  }
  return expr;
}

// Applicative order: leftmost-innermost (CBV)
function applicativeStep(expr) {
  if (expr.tag === 'App') {
    const fn = applicativeStep(expr.fn);
    if (fn !== expr.fn) return new App(fn, expr.arg);
    const arg = applicativeStep(expr.arg);
    if (arg !== expr.arg) return new App(expr.fn, arg);
    if (isRedex(expr)) return subst(expr.fn.body, expr.fn.var, expr.arg);
  }
  if (expr.tag === 'Lam') {
    const body = applicativeStep(expr.body);
    if (body !== expr.body) return new Lam(expr.var, body);
  }
  return expr;
}

function reduce(expr, stepFn, maxSteps = 100) {
  resetFresh();
  let current = expr;
  let steps = 0;
  const trace = [current.toString()];
  while (steps < maxSteps) {
    const next = stepFn(current);
    if (next === current) break;
    current = next;
    steps++;
    trace.push(current.toString());
  }
  return { result: current, steps, trace };
}

function normalReduce(expr, max) { return reduce(expr, normalStep, max); }
function applicativeReduce(expr, max) { return reduce(expr, applicativeStep, max); }

export { Var, Lam, App, subst, freeVars, isRedex, normalStep, applicativeStep, normalReduce, applicativeReduce, resetFresh };
