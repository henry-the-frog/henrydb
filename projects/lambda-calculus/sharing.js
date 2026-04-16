/**
 * Sharing Analysis: Detect shared subexpressions
 * 
 * In graph reduction, sharing is critical: if x is used twice,
 * we want to evaluate it once and share the result.
 * 
 * This module detects: usage counts, shared/unique references,
 * and optimal sharing strategies.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }

function usageCounts(expr, counts = new Map()) {
  switch (expr.tag) {
    case 'Var': counts.set(expr.name, (counts.get(expr.name) || 0) + 1); break;
    case 'Lam': usageCounts(expr.body, counts); break;
    case 'App': usageCounts(expr.fn, counts); usageCounts(expr.arg, counts); break;
    case 'Let': usageCounts(expr.init, counts); usageCounts(expr.body, counts); break;
  }
  return counts;
}

function classify(name, counts) {
  const c = counts.get(name) || 0;
  if (c === 0) return 'dead';     // Never used
  if (c === 1) return 'unique';   // Used once → inline safely
  return 'shared';                // Used multiple times → share
}

function deadVars(expr) {
  const counts = usageCounts(expr);
  const dead = [];
  function findBindings(e) {
    if (e.tag === 'Lam' && !counts.has(e.var)) dead.push(e.var);
    if (e.tag === 'Let' && !counts.has(e.var)) dead.push(e.var);
    if (e.tag === 'Lam') findBindings(e.body);
    if (e.tag === 'App') { findBindings(e.fn); findBindings(e.arg); }
    if (e.tag === 'Let') { findBindings(e.init); findBindings(e.body); }
  }
  findBindings(expr);
  return dead;
}

// Inline unique let bindings
function inlineUnique(expr) {
  if (expr.tag === 'Let') {
    const counts = usageCounts(expr.body);
    if ((counts.get(expr.var) || 0) <= 1) {
      return subst(inlineUnique(expr.body), expr.var, inlineUnique(expr.init));
    }
    return new Let(expr.var, inlineUnique(expr.init), inlineUnique(expr.body));
  }
  if (expr.tag === 'Lam') return new Lam(expr.var, inlineUnique(expr.body));
  if (expr.tag === 'App') return new App(inlineUnique(expr.fn), inlineUnique(expr.arg));
  return expr;
}

function subst(expr, name, repl) {
  switch (expr.tag) {
    case 'Var': return expr.name === name ? repl : expr;
    case 'Lam': return expr.var === name ? expr : new Lam(expr.var, subst(expr.body, name, repl));
    case 'App': return new App(subst(expr.fn, name, repl), subst(expr.arg, name, repl));
    case 'Let': return new Let(expr.var, subst(expr.init, name, repl), expr.var === name ? expr.body : subst(expr.body, name, repl));
    default: return expr;
  }
}

// Size metric
function exprSize(expr) {
  switch (expr.tag) {
    case 'Var': case 'Num': return 1;
    case 'Lam': return 1 + exprSize(expr.body);
    case 'App': return 1 + exprSize(expr.fn) + exprSize(expr.arg);
    case 'Let': return 1 + exprSize(expr.init) + exprSize(expr.body);
    default: return 1;
  }
}

export { Var, Lam, App, Let, Num, usageCounts, classify, deadVars, inlineUnique, exprSize };
