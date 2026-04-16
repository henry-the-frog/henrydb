/**
 * Cost Semantics: Assign costs to reductions
 * 
 * Instead of just "does this terminate?", ask "how expensive is it?"
 * Time = number of reductions, Space = max live allocations.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Add { constructor(l, r) { this.tag = 'Add'; this.left = l; this.right = r; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }

function subst(expr, name, repl) {
  switch (expr.tag) {
    case 'Var': return expr.name === name ? repl : expr;
    case 'Num': return expr;
    case 'Lam': return expr.var === name ? expr : new Lam(expr.var, subst(expr.body, name, repl));
    case 'App': return new App(subst(expr.fn, name, repl), subst(expr.arg, name, repl));
    case 'Add': return new Add(subst(expr.left, name, repl), subst(expr.right, name, repl));
    case 'Let': return new Let(expr.var, subst(expr.init, name, repl), expr.var === name ? expr.body : subst(expr.body, name, repl));
  }
}

class CostEval {
  constructor() { this.betaReductions = 0; this.allocations = 0; this.lookups = 0; this.additions = 0; }
  
  eval(expr, env = new Map()) {
    switch (expr.tag) {
      case 'Num': return expr.n;
      case 'Var': { this.lookups++; const v = env.get(expr.name); if (v === undefined) throw new Error(`Unbound: ${expr.name}`); return v; }
      case 'Lam': { this.allocations++; return { tag: 'Closure', var: expr.var, body: expr.body, env: new Map(env) }; }
      case 'App': {
        const fn = this.eval(expr.fn, env);
        const arg = this.eval(expr.arg, env);
        if (fn.tag !== 'Closure') throw new Error('Not a function');
        this.betaReductions++;
        return this.eval(fn.body, new Map([...fn.env, [fn.var, arg]]));
      }
      case 'Add': { this.additions++; return this.eval(expr.left, env) + this.eval(expr.right, env); }
      case 'Let': {
        const val = this.eval(expr.init, env);
        this.allocations++;
        return this.eval(expr.body, new Map([...env, [expr.var, val]]));
      }
    }
  }
  
  run(expr) {
    const result = this.eval(expr);
    return { result, costs: { beta: this.betaReductions, alloc: this.allocations, lookup: this.lookups, add: this.additions, total: this.betaReductions + this.allocations + this.lookups + this.additions } };
  }
}

// Static cost estimation (without evaluation)
function estimateCost(expr) {
  switch (expr.tag) {
    case 'Num': case 'Var': return 1;
    case 'Lam': return 1 + estimateCost(expr.body);
    case 'App': return 1 + estimateCost(expr.fn) + estimateCost(expr.arg);
    case 'Add': return 1 + estimateCost(expr.left) + estimateCost(expr.right);
    case 'Let': return 1 + estimateCost(expr.init) + estimateCost(expr.body);
    default: return 1;
  }
}

export { Var, Num, Lam, App, Add, Let, CostEval, estimateCost };
