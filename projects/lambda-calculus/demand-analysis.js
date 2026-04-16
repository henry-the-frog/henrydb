/**
 * Demand Analysis: How much of each argument is needed
 * 
 * Beyond strict/lazy: HOW MUCH of a value is actually used?
 * - Absent: not used at all
 * - Head strict: only outermost constructor evaluated
 * - Strict: fully evaluated
 * - Call demand: function applied to N args
 */

const D_ABSENT = { tag: 'Absent' };
const D_LAZY = { tag: 'Lazy' };
const D_HEAD = { tag: 'Head' };        // WHNF only
const D_STRICT = { tag: 'Strict' };     // Fully evaluated  
const D_SEQ = { tag: 'Seq' };           // Evaluated then discarded

class DCall { constructor(arity) { this.tag = 'Call'; this.arity = arity; } }

// Expression types
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class Case { constructor(scrut, alts) { this.tag = 'Case'; this.scrut = scrut; this.alts = alts; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }

function analyzeDemand(expr, varName) {
  switch (expr.tag) {
    case 'Var': return expr.name === varName ? D_STRICT : D_ABSENT;
    case 'Num': return D_ABSENT;
    case 'App': {
      const fnDemand = analyzeDemand(expr.fn, varName);
      const argDemand = analyzeDemand(expr.arg, varName);
      // If variable is the function being applied, it's a call demand
      if (expr.fn.tag === 'Var' && expr.fn.name === varName) {
        return new DCall(countAppArgs(expr));
      }
      return lubDemand(fnDemand, argDemand);
    }
    case 'Lam': return D_ABSENT; // Body not evaluated yet
    case 'Case': {
      const scrutDemand = analyzeDemand(expr.scrut, varName);
      // If scrutinee IS the variable, it's head-strict
      if (expr.scrut.tag === 'Var' && expr.scrut.name === varName) {
        return D_HEAD;
      }
      // Combine demands from alternatives
      let altDemand = D_ABSENT;
      for (const alt of expr.alts) {
        altDemand = lubDemand(altDemand, analyzeDemand(alt, varName));
      }
      return lubDemand(scrutDemand, altDemand);
    }
    case 'Let': {
      const initDemand = analyzeDemand(expr.init, varName);
      const bodyDemand = analyzeDemand(expr.body, varName);
      return lubDemand(initDemand, bodyDemand);
    }
    default: return D_ABSENT;
  }
}

function countAppArgs(expr) {
  let count = 0;
  let current = expr;
  while (current.tag === 'App') { count++; current = current.fn; }
  return count;
}

// Least upper bound of demands
function lubDemand(d1, d2) {
  if (d1.tag === 'Absent') return d2;
  if (d2.tag === 'Absent') return d1;
  if (d1.tag === 'Strict' || d2.tag === 'Strict') return D_STRICT;
  if (d1.tag === 'Head' || d2.tag === 'Head') return D_HEAD;
  if (d1.tag === 'Call' || d2.tag === 'Call') return d1.tag === 'Call' ? d1 : d2;
  return D_LAZY;
}

function demandString(d) {
  if (d.tag === 'Call') return `C(${d.arity})`;
  return d.tag;
}

export { D_ABSENT, D_LAZY, D_HEAD, D_STRICT, D_SEQ, DCall, Var, Num, App, Lam, Case, Let, analyzeDemand, lubDemand, demandString };
