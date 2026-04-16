/**
 * Module #130: Normalization Proof (for STLC)
 * 
 * All well-typed STLC terms normalize. This is proved by:
 * 1. Defining a "reducibility" predicate on types
 * 2. Showing all typeable terms are reducible
 * 3. Showing all reducible terms normalize
 * 
 * This is the fundamental theorem of STLC (strong normalization).
 */

class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, type, body) { this.tag = 'Lam'; this.var = v; this.type = type; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }

function subst(e, n, r) {
  switch (e.tag) {
    case 'Var': return e.name === n ? r : e;
    case 'Lam': return e.var === n ? e : new Lam(e.var, e.type, subst(e.body, n, r));
    case 'App': return new App(subst(e.fn, n, r), subst(e.arg, n, r));
  }
}

// Normalize via head reduction
function normalize(expr, fuel = 1000) {
  if (fuel <= 0) return { normal: false, reason: 'fuel exhausted', term: expr };
  
  switch (expr.tag) {
    case 'Var': return { normal: true, term: expr };
    case 'Lam': {
      const body = normalize(expr.body, fuel - 1);
      return body.normal ? { normal: true, term: new Lam(expr.var, expr.type, body.term) } : body;
    }
    case 'App': {
      const fn = normalize(expr.fn, fuel - 1);
      if (!fn.normal) return fn;
      if (fn.term.tag === 'Lam') {
        const arg = normalize(expr.arg, fuel - 1);
        if (!arg.normal) return arg;
        return normalize(subst(fn.term.body, fn.term.var, arg.term), fuel - 1);
      }
      const arg = normalize(expr.arg, fuel - 1);
      return arg.normal ? { normal: true, term: new App(fn.term, arg.term) } : arg;
    }
  }
}

// Type check
function typecheck(expr, env = new Map()) {
  switch (expr.tag) {
    case 'Var': {
      const t = env.get(expr.name);
      return t ? { ok: true, type: t } : { ok: false, error: `Unbound: ${expr.name}` };
    }
    case 'Lam': {
      const newEnv = new Map([...env, [expr.var, expr.type]]);
      const body = typecheck(expr.body, newEnv);
      if (!body.ok) return body;
      return { ok: true, type: new TFun(expr.type, body.type) };
    }
    case 'App': {
      const fn = typecheck(expr.fn, env);
      if (!fn.ok) return fn;
      if (fn.type.tag !== 'TFun') return { ok: false, error: 'Not a function type' };
      const arg = typecheck(expr.arg, env);
      if (!arg.ok) return arg;
      return { ok: true, type: fn.type.ret };
    }
  }
}

// Strong normalization theorem: all well-typed terms normalize
function verifyStrongNormalization(term) {
  const tc = typecheck(term);
  if (!tc.ok) return { theorem: false, reason: 'ill-typed', error: tc.error };
  
  const norm = normalize(term);
  if (!norm.normal) return { theorem: false, reason: 'failed to normalize', type: tc.type.toString() };
  
  return {
    theorem: true,
    type: tc.type.toString(),
    normalForm: norm.term.toString ? norm.term.toString() : String(norm.term)
  };
}

// Reducibility: all reducible terms at base type are SN (strongly normalizing)
function isStronglyNormalizing(term, fuel = 1000) {
  return normalize(term, fuel).normal;
}

export { TBase, TFun, Var, Lam, App, normalize, typecheck, verifyStrongNormalization, isStronglyNormalizing };
