/**
 * Defunctionalization (Reynolds 1972)
 * 
 * Transform higher-order programs to first-order by:
 * 1. Replace each λ-abstraction with a unique tag + captured environment
 * 2. Create a single `apply` function that dispatches on the tag
 * 
 * Before: let f = λx. x + y in f(5)
 * After:  let f = Clos1(y) in apply(f, 5)
 *         where apply(Clos1(y), x) = x + y
 * 
 * This is how ML/Haskell compilers work internally — closures become tagged
 * data structures, and function application becomes a case dispatch.
 */

// ============================================================
// Source Language (higher-order)
// ============================================================

class SNum { constructor(n) { this.tag = 'SNum'; this.n = n; } toString() { return `${this.n}`; } }
class SVar { constructor(name) { this.tag = 'SVar'; this.name = name; } toString() { return this.name; } }
class SLam { constructor(id, param, body, freeVars) { this.tag = 'SLam'; this.id = id; this.param = param; this.body = body; this.freeVars = freeVars; } }
class SApp { constructor(fn, arg) { this.tag = 'SApp'; this.fn = fn; this.arg = arg; } }
class SLet { constructor(name, val, body) { this.tag = 'SLet'; this.name = name; this.val = val; this.body = body; } }
class SPrim { constructor(op, left, right) { this.tag = 'SPrim'; this.op = op; this.left = left; this.right = right; } }

// ============================================================
// Target Language (first-order)
// ============================================================

class TNum { constructor(n) { this.tag = 'TNum'; this.n = n; } toString() { return `${this.n}`; } }
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TClos { constructor(id, captured) { this.tag = 'TClos'; this.id = id; this.captured = captured; } toString() { return `Clos${this.id}(${this.captured.map(c => c.toString()).join(', ')})`; } }
class TApply { constructor(fn, arg) { this.tag = 'TApply'; this.fn = fn; this.arg = arg; } toString() { return `apply(${this.fn}, ${this.arg})`; } }
class TLet { constructor(name, val, body) { this.tag = 'TLet'; this.name = name; this.val = val; this.body = body; } }
class TPrim { constructor(op, left, right) { this.tag = 'TPrim'; this.op = op; this.left = left; this.right = right; } }

// ============================================================
// Defunctionalization Transform
// ============================================================

class Defunctionalizer {
  constructor() {
    this.closureId = 0;
    this.applyCases = [];  // [{id, param, freeVars, body}]
  }

  /**
   * Transform a source expression to first-order target
   */
  transform(expr) {
    const result = this._transform(expr);
    return {
      program: result,
      applyCases: this.applyCases
    };
  }

  _transform(expr) {
    switch (expr.tag) {
      case 'SNum': return new TNum(expr.n);
      case 'SVar': return new TVar(expr.name);
      
      case 'SLam': {
        const id = this.closureId++;
        const freeVars = expr.freeVars || [];
        
        // Transform the body
        const transformedBody = this._transform(expr.body);
        
        // Record the apply case
        this.applyCases.push({
          id,
          param: expr.param,
          freeVars: [...freeVars],
          body: transformedBody
        });
        
        // Replace lambda with closure constructor
        return new TClos(id, freeVars.map(v => new TVar(v)));
      }
      
      case 'SApp': {
        const fn = this._transform(expr.fn);
        const arg = this._transform(expr.arg);
        return new TApply(fn, arg);
      }
      
      case 'SLet': {
        const val = this._transform(expr.val);
        const body = this._transform(expr.body);
        return new TLet(expr.name, val, body);
      }
      
      case 'SPrim': {
        const left = this._transform(expr.left);
        const right = this._transform(expr.right);
        return new TPrim(expr.op, left, right);
      }
      
      default:
        throw new Error(`Unknown source expression: ${expr.tag}`);
    }
  }

  /**
   * Generate the apply function as code
   */
  generateApply() {
    const lines = ['function apply(closure, arg) {'];
    lines.push('  switch (closure.id) {');
    for (const c of this.applyCases) {
      const params = c.freeVars.map((v, i) => `    const ${v} = closure.captured[${i}];`);
      lines.push(`    case ${c.id}: {`);
      lines.push(`      const ${c.param} = arg;`);
      for (const p of params) lines.push(p);
      lines.push(`      return ${this._toJS(c.body)};`);
      lines.push(`    }`);
    }
    lines.push('  }');
    lines.push('}');
    return lines.join('\n');
  }

  _toJS(expr) {
    switch (expr.tag) {
      case 'TNum': return `${expr.n}`;
      case 'TVar': return expr.name;
      case 'TClos': return `{id: ${expr.id}, captured: [${expr.captured.map(c => this._toJS(c)).join(', ')}]}`;
      case 'TApply': return `apply(${this._toJS(expr.fn)}, ${this._toJS(expr.arg)})`;
      case 'TLet': return `(function() { const ${expr.name} = ${this._toJS(expr.val)}; return ${this._toJS(expr.body)}; })()`;
      case 'TPrim': return `(${this._toJS(expr.left)} ${expr.op} ${this._toJS(expr.right)})`;
      default: return '???';
    }
  }
}

// ============================================================
// Evaluator for defunctionalized programs
// ============================================================

function evalDefunc(expr, env = new Map()) {
  switch (expr.tag) {
    case 'TNum': return expr.n;
    case 'TVar': {
      if (!env.has(expr.name)) throw new Error(`Unbound: ${expr.name}`);
      return env.get(expr.name);
    }
    case 'TClos': {
      const captured = expr.captured.map(c => evalDefunc(c, env));
      return { tag: 'closure', id: expr.id, captured };
    }
    case 'TApply': {
      const fn = evalDefunc(expr.fn, env);
      const arg = evalDefunc(expr.arg, env);
      if (fn.tag !== 'closure') throw new Error('Not a closure');
      return fn; // Need the apply function
    }
    case 'TLet': {
      const val = evalDefunc(expr.val, env);
      const newEnv = new Map(env);
      newEnv.set(expr.name, val);
      return evalDefunc(expr.body, newEnv);
    }
    case 'TPrim': {
      const l = evalDefunc(expr.left, env);
      const r = evalDefunc(expr.right, env);
      switch (expr.op) {
        case '+': return l + r;
        case '-': return l - r;
        case '*': return l * r;
        default: throw new Error(`Unknown op: ${expr.op}`);
      }
    }
    default: throw new Error(`Unknown: ${expr.tag}`);
  }
}

/**
 * Full evaluation: defunctionalize then evaluate with apply dispatch
 */
function evalWithApply(sourceExpr) {
  const defunc = new Defunctionalizer();
  const { program, applyCases } = defunc.transform(sourceExpr);
  
  function apply(closure, arg) {
    const kase = applyCases.find(c => c.id === closure.id);
    if (!kase) throw new Error(`Unknown closure id: ${closure.id}`);
    const env = new Map();
    for (let i = 0; i < kase.freeVars.length; i++) {
      env.set(kase.freeVars[i], closure.captured[i]);
    }
    env.set(kase.param, arg);
    return evaluate(kase.body, env, apply);
  }
  
  function evaluate(expr, env, applyFn) {
    switch (expr.tag) {
      case 'TNum': return expr.n;
      case 'TVar': return env.get(expr.name);
      case 'TClos': return { tag: 'closure', id: expr.id, captured: expr.captured.map(c => evaluate(c, env, applyFn)) };
      case 'TApply': {
        const fn = evaluate(expr.fn, env, applyFn);
        const arg = evaluate(expr.arg, env, applyFn);
        return applyFn(fn, arg);
      }
      case 'TLet': {
        const val = evaluate(expr.val, env, applyFn);
        const newEnv = new Map(env);
        newEnv.set(expr.name, val);
        return evaluate(expr.body, newEnv, applyFn);
      }
      case 'TPrim': {
        const l = evaluate(expr.left, env, applyFn);
        const r = evaluate(expr.right, env, applyFn);
        switch (expr.op) { case '+': return l + r; case '-': return l - r; case '*': return l * r; }
      }
    }
  }
  
  return evaluate(program, new Map(), apply);
}

// ============================================================
// Convenience constructors
// ============================================================

const snum = n => new SNum(n);
const svar = name => new SVar(name);
const slam = (param, body, freeVars = []) => new SLam(0, param, body, freeVars);
const sapp = (fn, arg) => new SApp(fn, arg);
const slet = (name, val, body) => new SLet(name, val, body);
const sprim = (op, l, r) => new SPrim(op, l, r);

export {
  SNum, SVar, SLam, SApp, SLet, SPrim,
  TNum, TVar, TClos, TApply, TLet, TPrim,
  Defunctionalizer, evalWithApply,
  snum, svar, slam, sapp, slet, sprim
};
