/**
 * Closure Conversion
 * 
 * Makes closure creation explicit in the program:
 * λx. body  →  makeClosure(code, [free vars])
 * 
 * Where `code` is a top-level function that takes an environment + parameter.
 * This is how real compilers handle closures — they become struct + function pointer.
 * 
 * Steps:
 * 1. Compute free variables of each lambda
 * 2. Replace lambda with closure construction
 * 3. Lift code to top level
 * 4. Replace variable references to captured vars with env lookups
 */

// ============================================================
// Source Language
// ============================================================

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(param, body) { this.tag = 'Lam'; this.param = param; this.body = body; } toString() { return `(λ${this.param}. ${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Prim { constructor(op, l, r) { this.tag = 'Prim'; this.op = op; this.l = l; this.r = r; } }
class Let { constructor(name, val, body) { this.tag = 'Let'; this.name = name; this.val = val; this.body = body; } }

// ============================================================
// Target Language (closure-converted)
// ============================================================

class CVar { constructor(name) { this.tag = 'CVar'; this.name = name; } toString() { return this.name; } }
class CNum { constructor(n) { this.tag = 'CNum'; this.n = n; } toString() { return `${this.n}`; } }
class CMakeClosure { constructor(codeLabel, captured) { this.tag = 'CMakeClosure'; this.codeLabel = codeLabel; this.captured = captured; } toString() { return `MkClos(${this.codeLabel}, [${this.captured.join(', ')}])`; } }
class CAppClosure { constructor(fn, arg) { this.tag = 'CAppClosure'; this.fn = fn; this.arg = arg; } toString() { return `appClos(${this.fn}, ${this.arg})`; } }
class CEnvRef { constructor(index) { this.tag = 'CEnvRef'; this.index = index; } toString() { return `env[${this.index}]`; } }
class CPrim { constructor(op, l, r) { this.tag = 'CPrim'; this.op = op; this.l = l; this.r = r; } }
class CLet { constructor(name, val, body) { this.tag = 'CLet'; this.name = name; this.val = val; this.body = body; } }

// Top-level code (lifted function)
class TopFun {
  constructor(label, envSize, param, body) {
    this.label = label;
    this.envSize = envSize;
    this.param = param;
    this.body = body;
  }
  toString() { return `fun ${this.label}(env[${this.envSize}], ${this.param}) = ${this.body}`; }
}

// ============================================================
// Free Variables
// ============================================================

function freeVars(expr) {
  switch (expr.tag) {
    case 'Var': return new Set([expr.name]);
    case 'Num': return new Set();
    case 'Lam': {
      const bodyFV = freeVars(expr.body);
      bodyFV.delete(expr.param);
      return bodyFV;
    }
    case 'App': return union(freeVars(expr.fn), freeVars(expr.arg));
    case 'Prim': return union(freeVars(expr.l), freeVars(expr.r));
    case 'Let': {
      const valFV = freeVars(expr.val);
      const bodyFV = freeVars(expr.body);
      bodyFV.delete(expr.name);
      return union(valFV, bodyFV);
    }
    default: return new Set();
  }
}

function union(a, b) { return new Set([...a, ...b]); }

// ============================================================
// Closure Conversion
// ============================================================

class ClosureConverter {
  constructor() {
    this.topFuns = [];
    this.labelCount = 0;
  }

  convert(expr) {
    const result = this._convert(expr, new Map());
    return {
      main: result,
      topFuns: this.topFuns
    };
  }

  _convert(expr, envMap) {
    switch (expr.tag) {
      case 'Num': return new CNum(expr.n);
      
      case 'Var': {
        // Check if this is a captured variable (in env)
        if (envMap.has(expr.name)) {
          return new CEnvRef(envMap.get(expr.name));
        }
        return new CVar(expr.name);
      }
      
      case 'Lam': {
        // Compute free variables
        const fv = [...freeVars(expr)];
        const label = `f${this.labelCount++}`;
        
        // Create env map for the body
        const bodyEnvMap = new Map();
        for (let i = 0; i < fv.length; i++) {
          bodyEnvMap.set(fv[i], i);
        }
        
        // Convert the body with env references
        const convertedBody = this._convert(expr.body, bodyEnvMap);
        
        // Lift to top level
        this.topFuns.push(new TopFun(label, fv.length, expr.param, convertedBody));
        
        // Replace with closure construction
        const capturedVars = fv.map(v => {
          if (envMap.has(v)) return new CEnvRef(envMap.get(v));
          return new CVar(v);
        });
        
        return new CMakeClosure(label, capturedVars);
      }
      
      case 'App': {
        const fn = this._convert(expr.fn, envMap);
        const arg = this._convert(expr.arg, envMap);
        return new CAppClosure(fn, arg);
      }
      
      case 'Prim': {
        const l = this._convert(expr.l, envMap);
        const r = this._convert(expr.r, envMap);
        return new CPrim(expr.op, l, r);
      }
      
      case 'Let': {
        const val = this._convert(expr.val, envMap);
        const body = this._convert(expr.body, envMap);
        return new CLet(expr.name, val, body);
      }
      
      default:
        throw new Error(`Unknown: ${expr.tag}`);
    }
  }
}

// ============================================================
// Evaluator for closure-converted programs
// ============================================================

function evalCC(program) {
  const { main, topFuns } = program;
  const funMap = new Map(topFuns.map(f => [f.label, f]));
  
  function eval_(expr, env) {
    switch (expr.tag) {
      case 'CNum': return expr.n;
      case 'CVar': return env.get(expr.name);
      case 'CEnvRef': return env.get(`__env__`)?.[expr.index];
      case 'CMakeClosure': {
        const captured = expr.captured.map(c => eval_(c, env));
        return { tag: 'closure', label: expr.codeLabel, env: captured };
      }
      case 'CAppClosure': {
        const fn = eval_(expr.fn, env);
        const arg = eval_(expr.arg, env);
        const fun = funMap.get(fn.label);
        const newEnv = new Map();
        newEnv.set('__env__', fn.env);
        newEnv.set(fun.param, arg);
        return eval_(fun.body, newEnv);
      }
      case 'CPrim': {
        const l = eval_(expr.l, env);
        const r = eval_(expr.r, env);
        switch (expr.op) { case '+': return l + r; case '-': return l - r; case '*': return l * r; }
      }
      case 'CLet': {
        const val = eval_(expr.val, env);
        const newEnv = new Map(env);
        newEnv.set(expr.name, val);
        return eval_(expr.body, newEnv);
      }
    }
  }
  
  return eval_(main, new Map());
}

// ============================================================
// Exports
// ============================================================

export {
  Var, Lam, App, Num, Prim, Let,
  CVar, CNum, CMakeClosure, CAppClosure, CEnvRef, CPrim, CLet, TopFun,
  ClosureConverter, freeVars, evalCC
};
