/**
 * Call Arity: Optimal evaluation order for recursive functions
 * 
 * In lazy evaluation, a recursive function might benefit from
 * being called with a specific number of arguments to avoid
 * building unnecessary thunks.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }
class If { constructor(c, t, f) { this.tag = 'If'; this.cond = c; this.then = t; this.else = f; } }

// Count nested lambdas
function arity(expr) {
  let n = 0, e = expr;
  while (e.tag === 'Lam') { n++; e = e.body; }
  return n;
}

// Count how many args an expression is applied to
function appArity(expr) {
  let n = 0, e = expr;
  while (e.tag === 'App') { n++; e = e.fn; }
  return { args: n, head: e };
}

// Analyze call arity for a recursive function
function analyzeCallArity(fnName, body) {
  const callArities = [];
  
  function walk(expr) {
    if (expr.tag === 'App') {
      const { args, head } = appArity(expr);
      if (head.tag === 'Var' && head.name === fnName) {
        callArities.push(args);
      }
      // Walk sub-expressions
      let current = expr;
      while (current.tag === 'App') {
        walk(current.arg);
        current = current.fn;
      }
      return;
    }
    if (expr.tag === 'Lam') walk(expr.body);
    if (expr.tag === 'Let') { walk(expr.init); walk(expr.body); }
    if (expr.tag === 'If') { walk(expr.cond); walk(expr.then); walk(expr.else); }
  }
  
  walk(body);
  return {
    callSites: callArities.length,
    arities: callArities,
    minimum: callArities.length ? Math.min(...callArities) : 0,
    maximum: callArities.length ? Math.max(...callArities) : 0,
    consistent: callArities.length ? new Set(callArities).size === 1 : true
  };
}

// Suggest optimal arity
function suggestArity(fnName, definition) {
  const manifest = arity(definition);
  const analysis = analyzeCallArity(fnName, definition);
  
  if (analysis.callSites === 0) return { optimal: manifest, reason: 'no recursive calls' };
  if (analysis.consistent) return { optimal: analysis.minimum, reason: `all calls use ${analysis.minimum} args` };
  return { optimal: analysis.minimum, reason: `mixed arities (${[...new Set(analysis.arities)].join(',')}), using minimum` };
}

export { Var, Num, Lam, App, Let, If, arity, appArity, analyzeCallArity, suggestArity };
