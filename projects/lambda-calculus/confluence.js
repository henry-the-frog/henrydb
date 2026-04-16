/**
 * Confluence (Church-Rosser Property)
 * 
 * If M →* N₁ and M →* N₂, then ∃P such that N₁ →* P and N₂ →* P.
 * "No matter how you reduce, you can always reach the same result."
 * 
 * This means: if a normal form exists, it's UNIQUE.
 * Beta reduction is confluent (Church-Rosser theorem, 1936).
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }

function subst(expr, name, repl) {
  switch (expr.tag) {
    case 'Var': return expr.name === name ? repl : expr;
    case 'Lam': return expr.var === name ? expr : new Lam(expr.var, subst(expr.body, name, repl));
    case 'App': return new App(subst(expr.fn, name, repl), subst(expr.arg, name, repl));
  }
}

// Different reduction strategies
function leftmostOutermost(expr) {
  if (expr.tag === 'App' && expr.fn.tag === 'Lam') return subst(expr.fn.body, expr.fn.var, expr.arg);
  if (expr.tag === 'App') { const fn = leftmostOutermost(expr.fn); return fn !== expr.fn ? new App(fn, expr.arg) : new App(expr.fn, leftmostOutermost(expr.arg)); }
  if (expr.tag === 'Lam') { const body = leftmostOutermost(expr.body); return body !== expr.body ? new Lam(expr.var, body) : expr; }
  return expr;
}

function rightmostInnermost(expr) {
  if (expr.tag === 'App') {
    const arg = rightmostInnermost(expr.arg);
    if (arg !== expr.arg) return new App(expr.fn, arg);
    const fn = rightmostInnermost(expr.fn);
    if (fn !== expr.fn) return new App(fn, expr.arg);
    if (expr.fn.tag === 'Lam') return subst(expr.fn.body, expr.fn.var, expr.arg);
  }
  if (expr.tag === 'Lam') { const body = rightmostInnermost(expr.body); return body !== expr.body ? new Lam(expr.var, body) : expr; }
  return expr;
}

function normalize(expr, stepFn, maxSteps = 100) {
  let current = expr, steps = 0;
  while (steps < maxSteps) { const next = stepFn(current); if (next === current || next.toString() === current.toString()) break; current = next; steps++; }
  return current;
}

// Check confluence empirically: both strategies reach same normal form
function checkConfluence(expr) {
  const nf1 = normalize(expr, leftmostOutermost);
  const nf2 = normalize(expr, rightmostInnermost);
  return { confluent: nf1.toString() === nf2.toString(), nf1: nf1.toString(), nf2: nf2.toString() };
}

// Parallel reduction (one-step parallel beta)
function parallelReduce(expr) {
  switch (expr.tag) {
    case 'Var': return expr;
    case 'Lam': return new Lam(expr.var, parallelReduce(expr.body));
    case 'App': {
      const fn = parallelReduce(expr.fn);
      const arg = parallelReduce(expr.arg);
      if (fn.tag === 'Lam') return subst(fn.body, fn.var, arg);
      return new App(fn, arg);
    }
  }
}

function isNormalForm(expr) {
  if (expr.tag === 'Var') return true;
  if (expr.tag === 'Lam') return isNormalForm(expr.body);
  if (expr.tag === 'App') return expr.fn.tag !== 'Lam' && isNormalForm(expr.fn) && isNormalForm(expr.arg);
  return true;
}

export { Var, Lam, App, subst, leftmostOutermost, rightmostInnermost, normalize, checkConfluence, parallelReduce, isNormalForm };
