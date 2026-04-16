/**
 * Hindley-Milner Type Inference (Algorithm W)
 * 
 * Complete implementation for pure lambda calculus + let:
 * - Type variables, function types, base types (Int, Bool, String)
 * - Unification with occurs check
 * - Algorithm W: constraint-based type inference
 * - Let-polymorphism: generalize and instantiate type schemes
 * 
 * This is the theoretical foundation that monkey-lang's type checker builds on.
 * Here we implement it for a minimal language to show the core algorithm clearly.
 * 
 * Based on:
 * - Milner (1978): A Theory of Type Polymorphism in Programming
 * - Damas & Milner (1982): Principal type-schemes for functional programs
 */

// ============================================================
// Types
// ============================================================

class TVar {
  constructor(name) { this.tag = 'TVar'; this.name = name; }
  toString() { return this.name; }
}

class TFun {
  constructor(param, ret) { this.tag = 'TFun'; this.param = param; this.ret = ret; }
  toString() { return `(${this.param} → ${this.ret})`; }
}

class TCon {
  constructor(name) { this.tag = 'TCon'; this.name = name; }
  toString() { return this.name; }
}

const tInt = new TCon('Int');
const tBool = new TCon('Bool');
const tStr = new TCon('String');

// ============================================================
// Type Schemes: ∀α₁...αₙ. τ
// ============================================================

class Scheme {
  constructor(vars, type) { this.vars = vars; this.type = type; }
  toString() { return this.vars.length > 0 ? `∀${this.vars.join(' ')}.${this.type}` : `${this.type}`; }
}

// ============================================================
// Expressions
// ============================================================

class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class ELam { constructor(param, body) { this.tag = 'ELam'; this.param = param; this.body = body; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class ELet { constructor(name, val, body) { this.tag = 'ELet'; this.name = name; this.val = val; this.body = body; } }
class ELit { constructor(type) { this.tag = 'ELit'; this.type = type; } }

// Convenience
const evar = name => new EVar(name);
const elam = (p, b) => new ELam(p, b);
const eapp = (f, a) => new EApp(f, a);
const elet = (n, v, b) => new ELet(n, v, b);
const eint = new ELit(tInt);
const ebool = new ELit(tBool);
const estr = new ELit(tStr);

// ============================================================
// Substitution
// ============================================================

class Subst {
  constructor(map = new Map()) { this.map = map; }
  
  apply(type) {
    switch (type.tag) {
      case 'TVar':
        if (this.map.has(type.name)) return this.apply(this.map.get(type.name));
        return type;
      case 'TFun':
        return new TFun(this.apply(type.param), this.apply(type.ret));
      case 'TCon':
        return type;
      default:
        return type;
    }
  }
  
  applyScheme(scheme) {
    // Don't substitute bound variables
    const restricted = new Map(this.map);
    for (const v of scheme.vars) restricted.delete(v);
    return new Scheme(scheme.vars, new Subst(restricted).apply(scheme.type));
  }
  
  compose(other) {
    // Apply this to all values in other, then merge
    const newMap = new Map();
    for (const [k, v] of other.map) {
      newMap.set(k, this.apply(v));
    }
    for (const [k, v] of this.map) {
      if (!newMap.has(k)) newMap.set(k, v);
    }
    return new Subst(newMap);
  }
}

// ============================================================
// Free Type Variables
// ============================================================

function ftv(type) {
  switch (type.tag) {
    case 'TVar': return new Set([type.name]);
    case 'TFun': return new Set([...ftv(type.param), ...ftv(type.ret)]);
    case 'TCon': return new Set();
    default: return new Set();
  }
}

function ftvScheme(scheme) {
  const bodyFtv = ftv(scheme.type);
  for (const v of scheme.vars) bodyFtv.delete(v);
  return bodyFtv;
}

function ftvEnv(env) {
  const result = new Set();
  for (const [, scheme] of env) {
    for (const v of ftvScheme(scheme)) result.add(v);
  }
  return result;
}

// ============================================================
// Unification
// ============================================================

function unify(t1, t2) {
  t1 = resolve(t1);
  t2 = resolve(t2);
  
  if (t1.tag === 'TVar' && t2.tag === 'TVar' && t1.name === t2.name) {
    return new Subst();
  }
  
  if (t1.tag === 'TVar') {
    if (occursIn(t1.name, t2)) {
      throw new Error(`Infinite type: ${t1.name} occurs in ${t2}`);
    }
    return new Subst(new Map([[t1.name, t2]]));
  }
  
  if (t2.tag === 'TVar') {
    return unify(t2, t1);
  }
  
  if (t1.tag === 'TCon' && t2.tag === 'TCon') {
    if (t1.name === t2.name) return new Subst();
    throw new Error(`Cannot unify ${t1} with ${t2}`);
  }
  
  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    const s1 = unify(t1.param, t2.param);
    const s2 = unify(s1.apply(t1.ret), s1.apply(t2.ret));
    return s2.compose(s1);
  }
  
  throw new Error(`Cannot unify ${t1} with ${t2}`);
}

function resolve(t) { return t; } // No indirection; substitution handles it

function occursIn(name, type) {
  switch (type.tag) {
    case 'TVar': return type.name === name;
    case 'TFun': return occursIn(name, type.param) || occursIn(name, type.ret);
    case 'TCon': return false;
    default: return false;
  }
}

// ============================================================
// Algorithm W
// ============================================================

let freshCounter = 0;
function freshVar() { return new TVar(`t${freshCounter++}`); }
function resetFresh() { freshCounter = 0; }

function generalize(env, type) {
  const envFtv = ftvEnv(env);
  const typeFtv = ftv(type);
  const vars = [...typeFtv].filter(v => !envFtv.has(v));
  return new Scheme(vars, type);
}

function instantiate(scheme) {
  const subst = new Map();
  for (const v of scheme.vars) {
    subst.set(v, freshVar());
  }
  return new Subst(subst).apply(scheme.type);
}

function infer(expr, env = new Map()) {
  resetFresh();
  return algorithmW(expr, env);
}

function algorithmW(expr, env) {
  switch (expr.tag) {
    case 'ELit':
      return { subst: new Subst(), type: expr.type };
    
    case 'EVar': {
      const scheme = env.get(expr.name);
      if (!scheme) throw new Error(`Unbound variable: ${expr.name}`);
      return { subst: new Subst(), type: instantiate(scheme) };
    }
    
    case 'ELam': {
      const tv = freshVar();
      const newEnv = new Map(env);
      newEnv.set(expr.param, new Scheme([], tv));
      const { subst, type } = algorithmW(expr.body, newEnv);
      return { subst, type: new TFun(subst.apply(tv), type) };
    }
    
    case 'EApp': {
      const tv = freshVar();
      const { subst: s1, type: t1 } = algorithmW(expr.fn, env);
      const newEnv = applySubstToEnv(s1, env);
      const { subst: s2, type: t2 } = algorithmW(expr.arg, newEnv);
      const s3 = unify(s2.apply(t1), new TFun(t2, tv));
      return { subst: s3.compose(s2).compose(s1), type: s3.apply(tv) };
    }
    
    case 'ELet': {
      const { subst: s1, type: t1 } = algorithmW(expr.val, env);
      const newEnv = applySubstToEnv(s1, env);
      const scheme = generalize(newEnv, t1);
      newEnv.set(expr.name, scheme);
      const { subst: s2, type: t2 } = algorithmW(expr.body, newEnv);
      return { subst: s2.compose(s1), type: t2 };
    }
    
    default:
      throw new Error(`Unknown expression: ${expr.tag}`);
  }
}

function applySubstToEnv(subst, env) {
  const newEnv = new Map();
  for (const [k, v] of env) {
    newEnv.set(k, subst.applyScheme(v));
  }
  return newEnv;
}

// ============================================================
// Exports
// ============================================================

export {
  TVar, TFun, TCon, tInt, tBool, tStr,
  Scheme, Subst,
  EVar, ELam, EApp, ELet, ELit,
  evar, elam, eapp, elet, eint, ebool, estr,
  unify, ftv, ftvScheme, ftvEnv, occursIn,
  infer, algorithmW, generalize, instantiate,
  freshVar, resetFresh
};
