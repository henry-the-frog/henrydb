/**
 * Typed Elaboration: Surface Language → Core Language
 * 
 * Elaboration is the process of translating a user-friendly surface syntax
 * into a fully-explicit core language with all types filled in.
 * 
 * Surface: let id = x => x          (no type annotations)
 * Core:    let id : ∀a. a → a = Λa. λ(x:a). x  (fully annotated)
 * 
 * This is what real compilers do: Rust, Haskell, OCaml all elaborate.
 */

// Surface syntax
class SVar { constructor(name) { this.tag = 'SVar'; this.name = name; } }
class SLam { constructor(v, body) { this.tag = 'SLam'; this.var = v; this.body = body; } }
class SApp { constructor(fn, arg) { this.tag = 'SApp'; this.fn = fn; this.arg = arg; } }
class SLet { constructor(v, init, body) { this.tag = 'SLet'; this.var = v; this.init = init; this.body = body; } }
class SNum { constructor(n) { this.tag = 'SNum'; this.n = n; } }
class SBool { constructor(v) { this.tag = 'SBool'; this.v = v; } }
class SIf { constructor(c, t, f) { this.tag = 'SIf'; this.cond = c; this.then = t; this.else = f; } }
class SAnn { constructor(expr, type) { this.tag = 'SAnn'; this.expr = expr; this.type = type; } }

// Core syntax (fully annotated)
class CVar { constructor(name) { this.tag = 'CVar'; this.name = name; } toString() { return this.name; } }
class CLam { constructor(v, type, body) { this.tag = 'CLam'; this.var = v; this.type = type; this.body = body; } toString() { return `(λ(${this.var}:${this.type}).${this.body})`; } }
class CApp { constructor(fn, arg) { this.tag = 'CApp'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class CLet { constructor(v, type, init, body) { this.tag = 'CLet'; this.var = v; this.type = type; this.init = init; this.body = body; } }
class CNum { constructor(n) { this.tag = 'CNum'; this.n = n; } toString() { return `${this.n}`; } }
class CBool { constructor(v) { this.tag = 'CBool'; this.v = v; } toString() { return `${this.v}`; } }
class CIf { constructor(c, t, f) { this.tag = 'CIf'; this.cond = c; this.then = t; this.else = f; } }

// Types
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TInt { constructor() { this.tag = 'TInt'; } toString() { return 'Int'; } }
class TBool { constructor() { this.tag = 'TBool'; } toString() { return 'Bool'; } }

const tInt = new TInt();
const tBool = new TBool();

// ============================================================
// Elaborator with type inference
// ============================================================

let metaCounter = 0;
function freshMeta() { return new TVar(`?${metaCounter++}`); }
function resetMetas() { metaCounter = 0; }

class Elaborator {
  constructor() {
    this.subst = new Map();
    this.errors = [];
  }

  unify(t1, t2) {
    t1 = this.resolve(t1);
    t2 = this.resolve(t2);
    
    if (t1.tag === 'TVar' && t1.name.startsWith('?')) {
      this.subst.set(t1.name, t2);
      return true;
    }
    if (t2.tag === 'TVar' && t2.name.startsWith('?')) {
      this.subst.set(t2.name, t1);
      return true;
    }
    if (t1.tag === 'TInt' && t2.tag === 'TInt') return true;
    if (t1.tag === 'TBool' && t2.tag === 'TBool') return true;
    if (t1.tag === 'TFun' && t2.tag === 'TFun') {
      return this.unify(t1.param, t2.param) && this.unify(t1.ret, t2.ret);
    }
    if (t1.tag === 'TVar' && t2.tag === 'TVar' && t1.name === t2.name) return true;
    
    this.errors.push(`Cannot unify ${t1} with ${t2}`);
    return false;
  }

  resolve(t) {
    if (t.tag === 'TVar' && this.subst.has(t.name)) return this.resolve(this.subst.get(t.name));
    if (t.tag === 'TFun') return new TFun(this.resolve(t.param), this.resolve(t.ret));
    return t;
  }

  /**
   * Elaborate surface syntax → (core syntax, type)
   */
  elaborate(expr, env = new Map()) {
    switch (expr.tag) {
      case 'SNum': return { core: new CNum(expr.n), type: tInt };
      case 'SBool': return { core: new CBool(expr.v), type: tBool };
      case 'SVar': {
        const type = env.get(expr.name);
        if (!type) { this.errors.push(`Unbound: ${expr.name}`); return { core: new CVar(expr.name), type: freshMeta() }; }
        return { core: new CVar(expr.name), type };
      }
      case 'SLam': {
        const paramType = freshMeta();
        const bodyEnv = new Map([...env, [expr.var, paramType]]);
        const body = this.elaborate(expr.body, bodyEnv);
        return {
          core: new CLam(expr.var, this.resolve(paramType), body.core),
          type: new TFun(paramType, body.type)
        };
      }
      case 'SApp': {
        const fn = this.elaborate(expr.fn, env);
        const arg = this.elaborate(expr.arg, env);
        const retType = freshMeta();
        this.unify(fn.type, new TFun(arg.type, retType));
        return { core: new CApp(fn.core, arg.core), type: retType };
      }
      case 'SLet': {
        const init = this.elaborate(expr.init, env);
        const bodyEnv = new Map([...env, [expr.var, init.type]]);
        const body = this.elaborate(expr.body, bodyEnv);
        return {
          core: new CLet(expr.var, this.resolve(init.type), init.core, body.core),
          type: body.type
        };
      }
      case 'SIf': {
        const cond = this.elaborate(expr.cond, env);
        this.unify(cond.type, tBool);
        const then_ = this.elaborate(expr.then, env);
        const else_ = this.elaborate(expr.else, env);
        this.unify(then_.type, else_.type);
        return { core: new CIf(cond.core, then_.core, else_.core), type: then_.type };
      }
      case 'SAnn': {
        const inner = this.elaborate(expr.expr, env);
        this.unify(inner.type, expr.type);
        return inner;
      }
      default: throw new Error(`Unknown surface: ${expr.tag}`);
    }
  }
}

function elaborate(expr, env) {
  resetMetas();
  const elab = new Elaborator();
  const result = elab.elaborate(expr, env);
  return {
    core: result.core,
    type: elab.resolve(result.type),
    errors: elab.errors
  };
}

export {
  SVar, SLam, SApp, SLet, SNum, SBool, SIf, SAnn,
  CVar, CLam, CApp, CLet, CNum, CBool, CIf,
  TVar, TFun, TInt, TBool, tInt, tBool,
  Elaborator, elaborate, resetMetas
};
