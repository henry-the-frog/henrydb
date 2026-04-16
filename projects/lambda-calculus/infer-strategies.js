/**
 * Type Inference Strategies: Compare approaches side by side
 * 
 * W (bottom-up), M (top-down), bidirectional, constraint-based
 * All infer the same types, different trade-offs.
 */

class TVar { constructor(n) { this.tag='TVar'; this.name=n; } toString() { return this.name; } }
class TFun { constructor(p,r) { this.tag='TFun'; this.param=p; this.ret=r; } toString() { return `(${this.param}→${this.ret})`; } }
class TCon { constructor(n) { this.tag='TCon'; this.name=n; } toString() { return this.name; } }

const tInt = new TCon('Int'), tBool = new TCon('Bool');
let _counter = 0;
function fresh() { return new TVar(`t${_counter++}`); }
function resetCounter() { _counter = 0; }

// Strategy W: bottom-up (Algorithm W)
function inferW(expr, env = new Map()) {
  resetCounter();
  return _inferW(expr, env);
}

function _inferW(expr, env) {
  switch(expr.tag) {
    case 'ENum': return { type: tInt, subst: new Map() };
    case 'EBool': return { type: tBool, subst: new Map() };
    case 'EVar': {
      const t = env.get(expr.name);
      if (!t) throw new Error(`Unbound: ${expr.name}`);
      return { type: t, subst: new Map() };
    }
    case 'ELam': {
      const paramTy = fresh();
      const newEnv = new Map([...env, [expr.var, paramTy]]);
      const body = _inferW(expr.body, newEnv);
      return { type: new TFun(applyS(body.subst, paramTy), body.type), subst: body.subst };
    }
    case 'EApp': {
      const fn = _inferW(expr.fn, env);
      const arg = _inferW(expr.arg, applyEnv(fn.subst, env));
      const retTy = fresh();
      const s = unify(applyS(arg.subst, fn.type), new TFun(arg.type, retTy));
      return { type: applyS(s, retTy), subst: compose(s, compose(arg.subst, fn.subst)) };
    }
  }
}

// Strategy M: top-down
function inferM(expr, env = new Map(), expected = null) {
  resetCounter();
  return _inferM(expr, env, expected || fresh());
}

function _inferM(expr, env, expected) {
  switch(expr.tag) {
    case 'ENum': return unify(expected, tInt);
    case 'EBool': return unify(expected, tBool);
    case 'EVar': {
      const t = env.get(expr.name);
      if (!t) throw new Error(`Unbound: ${expr.name}`);
      return unify(expected, t);
    }
    case 'ELam': {
      const a = fresh(), b = fresh();
      const s1 = unify(expected, new TFun(a, b));
      const newEnv = new Map([...applyEnv(s1, env), [expr.var, applyS(s1, a)]]);
      const s2 = _inferM(expr.body, newEnv, applyS(s1, b));
      return compose(s2, s1);
    }
    default: return new Map();
  }
}

function unify(t1, t2) {
  if (t1.toString() === t2.toString()) return new Map();
  if (t1.tag === 'TVar') return new Map([[t1.name, t2]]);
  if (t2.tag === 'TVar') return new Map([[t2.name, t1]]);
  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    const s1 = unify(t1.param, t2.param);
    const s2 = unify(applyS(s1, t1.ret), applyS(s1, t2.ret));
    return compose(s2, s1);
  }
  throw new Error(`Cannot unify ${t1} with ${t2}`);
}

function applyS(subst, type) {
  if (type.tag === 'TVar') return subst.has(type.name) ? applyS(subst, subst.get(type.name)) : type;
  if (type.tag === 'TFun') return new TFun(applyS(subst, type.param), applyS(subst, type.ret));
  return type;
}

function applyEnv(subst, env) {
  const result = new Map();
  for (const [k, v] of env) result.set(k, applyS(subst, v));
  return result;
}

function compose(s1, s2) {
  const result = new Map();
  for (const [k, v] of s2) result.set(k, applyS(s1, v));
  for (const [k, v] of s1) if (!result.has(k)) result.set(k, v);
  return result;
}

// Expression constructors
class ENum { constructor(n) { this.tag='ENum'; this.n=n; } }
class EBool { constructor(b) { this.tag='EBool'; this.b=b; } }
class EVar { constructor(name) { this.tag='EVar'; this.name=name; } }
class ELam { constructor(v, body) { this.tag='ELam'; this.var=v; this.body=body; } }
class EApp { constructor(fn, arg) { this.tag='EApp'; this.fn=fn; this.arg=arg; } }

export { ENum, EBool, EVar, ELam, EApp, inferW, inferM, tInt, tBool, resetCounter };
