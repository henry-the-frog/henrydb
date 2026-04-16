/**
 * A-Normal Form (ANF) Conversion
 * 
 * Transform expressions so that all intermediate results are named.
 * No nested applications: f(g(x)) → let t = g(x) in f(t)
 * 
 * ANF makes evaluation order explicit and is the basis for many compiler IRs.
 * Used by: Scheme compilers, OCaml's Flambda, V8's Turbofan (SSA is similar).
 * 
 * Flanagan et al. (1993) "The Essence of Compiling with Continuations"
 */

// ============================================================
// Source Language
// ============================================================

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(param, body) { this.tag = 'Lam'; this.param = param; this.body = body; } toString() { return `(λ${this.param}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Prim { constructor(op, l, r) { this.tag = 'Prim'; this.op = op; this.l = l; this.r = r; } toString() { return `(${this.l} ${this.op} ${this.r})`; } }
class Let { constructor(name, val, body) { this.tag = 'Let'; this.name = name; this.val = val; this.body = body; } toString() { return `(let ${this.name} = ${this.val} in ${this.body})`; } }
class If { constructor(cond, then, els) { this.tag = 'If'; this.cond = cond; this.then = then; this.els = els; } }

// ============================================================
// ANF Language (target)
// ============================================================

// Atomic expressions (trivial, no side effects)
class AVar { constructor(name) { this.tag = 'AVar'; this.name = name; } toString() { return this.name; } }
class ANum { constructor(n) { this.tag = 'ANum'; this.n = n; } toString() { return `${this.n}`; } }
class ALam { constructor(param, body) { this.tag = 'ALam'; this.param = param; this.body = body; } toString() { return `(λ${this.param}.${this.body})`; } }

// Complex expressions (may have effects, must be let-bound)
class CApp { constructor(fn, arg) { this.tag = 'CApp'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class CPrim { constructor(op, l, r) { this.tag = 'CPrim'; this.op = op; this.l = l; this.r = r; } toString() { return `(${this.l} ${this.op} ${this.r})`; } }
class CIf { constructor(cond, then, els) { this.tag = 'CIf'; this.cond = cond; this.then = then; this.els = els; } }

// ANF let binding (binds a complex expression)
class ALet { constructor(name, complex, body) { this.tag = 'ALet'; this.name = name; this.complex = complex; this.body = body; } toString() { return `(let ${this.name} = ${this.complex} in ${this.body})`; } }

// ============================================================
// ANF Conversion
// ============================================================

let anfCounter = 0;
function freshName() { return `_t${anfCounter++}`; }
function resetAnf() { anfCounter = 0; }

/**
 * Convert an expression to ANF.
 * Returns an ANF expression.
 */
function toANF(expr) {
  resetAnf();
  return normalize(expr, x => x);
}

/**
 * Normalize with a continuation.
 * k: what to do with the resulting atomic value
 */
function normalize(expr, k) {
  switch (expr.tag) {
    case 'Num':
      return k(new ANum(expr.n));
    
    case 'Var':
      return k(new AVar(expr.name));
    
    case 'Lam': {
      const body = toANFInner(expr.body);
      return k(new ALam(expr.param, body));
    }
    
    case 'App':
      return normalizeName(expr.fn, fn =>
        normalizeName(expr.arg, arg => {
          const app = new CApp(fn, arg);
          const result = k(app);
          return result;
        }));
    
    case 'Prim':
      return normalizeName(expr.l, l =>
        normalizeName(expr.r, r => {
          const prim = new CPrim(expr.op, l, r);
          const result = k(prim);
          return result;
        }));
    
    case 'Let': {
      return normalize(expr.val, val => {
        if (isAtomic(val)) {
          // Simple value: just bind directly
          return new ALet(expr.name, val, normalize(expr.body, k));
        }
        return new ALet(expr.name, val, normalize(expr.body, k));
      });
    }
    
    case 'If':
      return normalizeName(expr.cond, cond => {
        const thenBranch = toANFInner(expr.then);
        const elseBranch = toANFInner(expr.els);
        const ifExpr = new CIf(cond, thenBranch, elseBranch);
        return letBind(ifExpr, k);
      });
    
    default:
      throw new Error(`ANF: unknown ${expr.tag}`);
  }
}

/**
 * Normalize to a name (atomic expression).
 * If already atomic, pass directly. Otherwise, let-bind.
 */
function normalizeName(expr, k) {
  return normalize(expr, val => {
    if (isAtomic(val)) return k(val);
    const name = freshName();
    return new ALet(name, val, k(new AVar(name)));
  });
}

function toANFInner(expr) {
  return normalize(expr, x => x);
}

function letBind(complex, k) {
  if (k === (x => x)) return complex;
  const name = freshName();
  return new ALet(name, complex, k(new AVar(name)));
}

function isAtomic(expr) {
  return expr.tag === 'AVar' || expr.tag === 'ANum' || expr.tag === 'ALam';
}

// ============================================================
// ANF Checker (validates ANF property)
// ============================================================

function isInANF(expr) {
  switch (expr.tag) {
    case 'AVar': case 'ANum': return true;
    case 'ALam': return isInANF(expr.body);
    case 'CApp': return isAtomic(expr.fn) && isAtomic(expr.arg);
    case 'CPrim': return isAtomic(expr.l) && isAtomic(expr.r);
    case 'CIf': return isAtomic(expr.cond) && isInANF(expr.then) && isInANF(expr.els);
    case 'ALet': return isInANF(expr.body); // complex part can be non-atomic
    default: return false;
  }
}

// ============================================================
// Simple evaluator for ANF
// ============================================================

function evalANF(expr, env = new Map()) {
  switch (expr.tag) {
    case 'ANum': return expr.n;
    case 'AVar': return env.get(expr.name);
    case 'ALam': return { tag: 'closure', param: expr.param, body: expr.body, env: new Map(env) };
    case 'CApp': {
      const fn = evalANF(expr.fn, env);
      const arg = evalANF(expr.arg, env);
      const newEnv = new Map(fn.env);
      newEnv.set(fn.param, arg);
      return evalANF(fn.body, newEnv);
    }
    case 'CPrim': {
      const l = evalANF(expr.l, env);
      const r = evalANF(expr.r, env);
      switch (expr.op) { case '+': return l + r; case '-': return l - r; case '*': return l * r; }
    }
    case 'ALet': {
      const val = evalANF(expr.complex, env);
      const newEnv = new Map(env);
      newEnv.set(expr.name, val);
      return evalANF(expr.body, newEnv);
    }
    case 'CIf': {
      const cond = evalANF(expr.cond, env);
      return cond ? evalANF(expr.then, env) : evalANF(expr.els, env);
    }
    default: throw new Error(`evalANF: ${expr.tag}`);
  }
}

export {
  Var, Lam, App, Num, Prim, Let, If,
  AVar, ANum, ALam, CApp, CPrim, CIf, ALet,
  toANF, isInANF, isAtomic, evalANF, resetAnf
};
