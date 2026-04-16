/**
 * Occurs Check: Prevent infinite types in unification
 * 
 * When unifying type variable α with type τ,
 * check if α occurs in τ. If so, unification would create
 * an infinite type (α = List α = List (List α) = ...).
 * 
 * This is a standalone module focused specifically on the
 * occurs check algorithm and its applications.
 */

class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TCon { constructor(name, args = []) { this.tag = 'TCon'; this.name = name; this.args = args; } toString() { return this.args.length ? `${this.name}<${this.args.join(',')}>` : this.name; } }

function occurs(varName, type) {
  switch (type.tag) {
    case 'TVar': return type.name === varName;
    case 'TFun': return occurs(varName, type.param) || occurs(varName, type.ret);
    case 'TCon': return type.args.some(a => occurs(varName, a));
    default: return false;
  }
}

function occursDeep(varName, type, subst = new Map()) {
  type = resolve(type, subst);
  return occurs(varName, type);
}

function resolve(type, subst) {
  if (type.tag === 'TVar' && subst.has(type.name)) return resolve(subst.get(type.name), subst);
  return type;
}

// Detect which variables would cause infinite types
function findInfiniteTypes(constraints) {
  const issues = [];
  for (const [t1, t2] of constraints) {
    if (t1.tag === 'TVar' && occurs(t1.name, t2)) {
      issues.push({ var: t1.name, type: t2.toString(), issue: 'infinite type' });
    }
    if (t2.tag === 'TVar' && occurs(t2.name, t1)) {
      issues.push({ var: t2.name, type: t1.toString(), issue: 'infinite type' });
    }
  }
  return issues;
}

// Unify with occurs check
function unifyWithCheck(t1, t2) {
  if (t1.tag === 'TVar' && t2.tag === 'TVar' && t1.name === t2.name) return { ok: true, subst: new Map() };
  if (t1.tag === 'TVar') {
    if (occurs(t1.name, t2)) return { ok: false, error: `Infinite type: ${t1.name} in ${t2}` };
    return { ok: true, subst: new Map([[t1.name, t2]]) };
  }
  if (t2.tag === 'TVar') {
    if (occurs(t2.name, t1)) return { ok: false, error: `Infinite type: ${t2.name} in ${t1}` };
    return { ok: true, subst: new Map([[t2.name, t1]]) };
  }
  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    const r1 = unifyWithCheck(t1.param, t2.param);
    if (!r1.ok) return r1;
    return unifyWithCheck(applySubst(r1.subst, t1.ret), applySubst(r1.subst, t2.ret));
  }
  if (t1.tag === 'TCon' && t2.tag === 'TCon' && t1.name === t2.name && t1.args.length === t2.args.length) {
    let subst = new Map();
    for (let i = 0; i < t1.args.length; i++) {
      const r = unifyWithCheck(applySubst(subst, t1.args[i]), applySubst(subst, t2.args[i]));
      if (!r.ok) return r;
      subst = new Map([...subst, ...r.subst]);
    }
    return { ok: true, subst };
  }
  return { ok: false, error: `Cannot unify ${t1} with ${t2}` };
}

function applySubst(subst, type) {
  switch (type.tag) {
    case 'TVar': return subst.has(type.name) ? applySubst(subst, subst.get(type.name)) : type;
    case 'TFun': return new TFun(applySubst(subst, type.param), applySubst(subst, type.ret));
    case 'TCon': return new TCon(type.name, type.args.map(a => applySubst(subst, a)));
    default: return type;
  }
}

export { TVar, TFun, TCon, occurs, occursDeep, findInfiniteTypes, unifyWithCheck, applySubst };
