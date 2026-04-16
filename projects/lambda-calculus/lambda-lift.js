/**
 * Lambda Lifting (Johnsson 1985)
 * 
 * Alternative to closure conversion: instead of making closures,
 * add free variables as extra parameters to the function.
 * 
 * Before: let f = λx. x + y in f(5)
 * After:  let f = λy.λx. x + y in f(y)(5)
 * 
 * All functions become closed (no free variables) and can be top-level.
 * Used in Haskell's GHC compiler (supercombinators).
 */

// ============================================================
// Expressions
// ============================================================

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(params, body) { this.tag = 'Lam'; this.params = params; this.body = body; } toString() { return `(λ${this.params.join(' ')}.${this.body})`; } }
class App { constructor(fn, args) { this.tag = 'App'; this.fn = fn; this.args = args; } toString() { return `(${this.fn} ${this.args.join(' ')})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Prim { constructor(op, l, r) { this.tag = 'Prim'; this.op = op; this.l = l; this.r = r; } toString() { return `(${this.l} ${this.op} ${this.r})`; } }
class Let { constructor(name, val, body) { this.tag = 'Let'; this.name = name; this.val = val; this.body = body; } toString() { return `(let ${this.name} = ${this.val} in ${this.body})`; } }

// Top-level definition after lifting
class TopDef {
  constructor(name, params, body) { this.name = name; this.params = params; this.body = body; }
  toString() { return `${this.name}(${this.params.join(', ')}) = ${this.body}`; }
}

// ============================================================
// Free Variables
// ============================================================

function freeVars(expr) {
  switch (expr.tag) {
    case 'Num': return new Set();
    case 'Var': return new Set([expr.name]);
    case 'Lam': {
      const bodyFv = freeVars(expr.body);
      for (const p of expr.params) bodyFv.delete(p);
      return bodyFv;
    }
    case 'App': {
      const result = freeVars(expr.fn);
      for (const a of expr.args) for (const v of freeVars(a)) result.add(v);
      return result;
    }
    case 'Prim': return new Set([...freeVars(expr.l), ...freeVars(expr.r)]);
    case 'Let': {
      const valFv = freeVars(expr.val);
      const bodyFv = freeVars(expr.body);
      bodyFv.delete(expr.name);
      return new Set([...valFv, ...bodyFv]);
    }
    default: return new Set();
  }
}

// ============================================================
// Lambda Lifter
// ============================================================

class LambdaLifter {
  constructor() {
    this.topDefs = [];
    this.nameCounter = 0;
  }

  freshName() { return `$f${this.nameCounter++}`; }

  /**
   * Lift all lambdas to top level
   */
  lift(expr) {
    const result = this._lift(expr);
    return { main: result, topDefs: this.topDefs };
  }

  _lift(expr) {
    switch (expr.tag) {
      case 'Num': return expr;
      case 'Var': return expr;
      
      case 'Lam': {
        // Compute free variables
        const fv = [...freeVars(expr)];
        
        // Lift body
        const liftedBody = this._lift(expr.body);
        
        // Create top-level definition with extra parameters
        const name = this.freshName();
        const allParams = [...fv, ...expr.params];
        this.topDefs.push(new TopDef(name, allParams, liftedBody));
        
        // Replace lambda with partial application of free vars
        if (fv.length === 0) {
          // No free vars: just reference the top-level name
          return new Var(name);
        }
        // With free vars: create application that supplies them
        return new App(new Var(name), fv.map(v => new Var(v)));
      }
      
      case 'App': {
        const fn = this._lift(expr.fn);
        const args = expr.args.map(a => this._lift(a));
        return new App(fn, args);
      }
      
      case 'Prim': {
        const l = this._lift(expr.l);
        const r = this._lift(expr.r);
        return new Prim(expr.op, l, r);
      }
      
      case 'Let': {
        const val = this._lift(expr.val);
        const body = this._lift(expr.body);
        return new Let(expr.name, val, body);
      }
      
      default:
        throw new Error(`Lambda lift: unknown ${expr.tag}`);
    }
  }
}

// ============================================================
// Evaluator (with top-level definitions)
// ============================================================

function evalLifted(program) {
  const { main, topDefs } = program;
  const defs = new Map(topDefs.map(d => [d.name, d]));
  
  function eval_(expr, env) {
    switch (expr.tag) {
      case 'Num': return expr.n;
      case 'Var': {
        if (env.has(expr.name)) return env.get(expr.name);
        // Could be a top-level function reference
        if (defs.has(expr.name)) return { tag: 'topfn', name: expr.name };
        throw new Error(`Unbound: ${expr.name}`);
      }
      case 'Lam': {
        return { tag: 'closure', params: expr.params, body: expr.body, env: new Map(env) };
      }
      case 'App': {
        const fn = eval_(expr.fn, env);
        const args = expr.args.map(a => eval_(a, env));
        return apply_(fn, args, env);
      }
      case 'Prim': {
        const l = eval_(expr.l, env);
        const r = eval_(expr.r, env);
        switch (expr.op) { case '+': return l + r; case '-': return l - r; case '*': return l * r; }
      }
      case 'Let': {
        const val = eval_(expr.val, env);
        const newEnv = new Map(env);
        newEnv.set(expr.name, val);
        return eval_(expr.body, newEnv);
      }
      default: throw new Error(`Eval: ${expr.tag}`);
    }
  }
  
  function apply_(fn, args, env) {
    if (fn.tag === 'topfn') {
      const def = defs.get(fn.name);
      if (args.length < def.params.length) {
        // Partial application
        return { tag: 'partial', name: fn.name, appliedArgs: args };
      }
      const newEnv = new Map();
      for (let i = 0; i < def.params.length; i++) {
        newEnv.set(def.params[i], args[i]);
      }
      return eval_(def.body, newEnv);
    }
    if (fn.tag === 'partial') {
      const allArgs = [...fn.appliedArgs, ...args];
      return apply_({ tag: 'topfn', name: fn.name }, allArgs, env);
    }
    if (fn.tag === 'closure') {
      const newEnv = new Map(fn.env);
      for (let i = 0; i < fn.params.length && i < args.length; i++) {
        newEnv.set(fn.params[i], args[i]);
      }
      return eval_(fn.body, newEnv);
    }
    throw new Error(`Cannot apply: ${JSON.stringify(fn)}`);
  }
  
  return eval_(main, new Map());
}

export {
  Var, Lam, App, Num, Prim, Let, TopDef,
  freeVars, LambdaLifter, evalLifted
};
