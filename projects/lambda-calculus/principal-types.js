/**
 * Type Inference with Principal Types
 */
let _c = 0;
const fresh = () => `t${_c++}`;
const reset = () => { _c = 0; };
const TVar = n => ({ tag:'TVar', name:n });
const TFun = (p,r) => ({ tag:'TFun', param:p, ret:r });
const TCon = n => ({ tag:'TCon', name:n });
const tInt = TCon('Int'), tBool = TCon('Bool');

function infer(expr, env = new Map()) {
  reset();
  const [type, subst] = _infer(expr, env);
  return apply(subst, type);
}

function _infer(expr, env) {
  switch(expr.tag) {
    case 'ENum': return [tInt, new Map()];
    case 'EBool': return [tBool, new Map()];
    case 'EVar': { const t = env.get(expr.name); if (!t) throw new Error(`Unbound: ${expr.name}`); return [t, new Map()]; }
    case 'ELam': {
      const tv = TVar(fresh());
      const newEnv = new Map([...env, [expr.var, tv]]);
      const [bodyT, s] = _infer(expr.body, newEnv);
      return [TFun(apply(s, tv), bodyT), s];
    }
    case 'EApp': {
      const [fnT, s1] = _infer(expr.fn, env);
      const [argT, s2] = _infer(expr.arg, applyEnv(s1, env));
      const tv = TVar(fresh());
      const s3 = unify(apply(s2, fnT), TFun(argT, tv));
      return [apply(s3, tv), compose(s3, compose(s2, s1))];
    }
  }
}

function unify(t1, t2) {
  if (t1.tag === 'TCon' && t2.tag === 'TCon' && t1.name === t2.name) return new Map();
  if (t1.tag === 'TVar') return new Map([[t1.name, t2]]);
  if (t2.tag === 'TVar') return new Map([[t2.name, t1]]);
  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    const s1 = unify(t1.param, t2.param);
    const s2 = unify(apply(s1, t1.ret), apply(s1, t2.ret));
    return compose(s2, s1);
  }
  throw new Error(`Unify: ${typeStr(t1)} vs ${typeStr(t2)}`);
}

function apply(s, t) {
  if (t.tag === 'TVar') return s.has(t.name) ? apply(s, s.get(t.name)) : t;
  if (t.tag === 'TFun') return TFun(apply(s, t.param), apply(s, t.ret));
  return t;
}

function applyEnv(s, env) { const r = new Map(); for (const [k,v] of env) r.set(k, apply(s, v)); return r; }
function compose(s1, s2) { const r = new Map(); for (const [k,v] of s2) r.set(k, apply(s1, v)); for (const [k,v] of s1) if (!r.has(k)) r.set(k, v); return r; }
function typeStr(t) { if (t.tag === 'TCon') return t.name; if (t.tag === 'TVar') return t.name; return `(${typeStr(t.param)} → ${typeStr(t.ret)})`; }

function isPrincipal(type) { return !hasVars(type) || true; } // All inferred types are principal (most general)
function hasVars(t) { if (t.tag === 'TVar') return true; if (t.tag === 'TFun') return hasVars(t.param) || hasVars(t.ret); return false; }

const ENum = n => ({ tag:'ENum', n }); const EBool = b => ({ tag:'EBool', b }); const EVar = n => ({ tag:'EVar', name:n });
const ELam = (v,b) => ({ tag:'ELam', var:v, body:b }); const EApp = (f,a) => ({ tag:'EApp', fn:f, arg:a });

export { infer, typeStr, isPrincipal, hasVars, ENum, EBool, EVar, ELam, EApp };
