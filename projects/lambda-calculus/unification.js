/**
 * Robinson's Unification Algorithm (1965)
 * 
 * Find a substitution σ such that σ(t1) = σ(t2).
 * The most general unifier (MGU) is the least committal such substitution.
 * 
 * With occurs check: prevents infinite types like a = a → a.
 */

// Types
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TCon { constructor(name, args = []) { this.tag = 'TCon'; this.name = name; this.args = args; } toString() { return this.args.length ? `${this.name}<${this.args.join(',')}>` : this.name; } }

// Substitution: Map<string, Type>
function applySubst(subst, type) {
  switch (type.tag) {
    case 'TVar': return subst.has(type.name) ? applySubst(subst, subst.get(type.name)) : type;
    case 'TFun': return new TFun(applySubst(subst, type.param), applySubst(subst, type.ret));
    case 'TCon': return new TCon(type.name, type.args.map(a => applySubst(subst, a)));
    default: return type;
  }
}

function composeSubst(s1, s2) {
  const result = new Map();
  for (const [k, v] of s2) result.set(k, applySubst(s1, v));
  for (const [k, v] of s1) if (!result.has(k)) result.set(k, v);
  return result;
}

// ============================================================
// Occurs check
// ============================================================

function occursIn(varName, type) {
  switch (type.tag) {
    case 'TVar': return type.name === varName;
    case 'TFun': return occursIn(varName, type.param) || occursIn(varName, type.ret);
    case 'TCon': return type.args.some(a => occursIn(varName, a));
    default: return false;
  }
}

// ============================================================
// Robinson's Unification
// ============================================================

function unify(t1, t2) {
  t1 = normalize(t1);
  t2 = normalize(t2);

  if (t1.tag === 'TVar' && t2.tag === 'TVar' && t1.name === t2.name) {
    return { ok: true, subst: new Map() };
  }

  if (t1.tag === 'TVar') {
    if (occursIn(t1.name, t2)) return { ok: false, error: `Occurs check: ${t1.name} in ${t2}` };
    return { ok: true, subst: new Map([[t1.name, t2]]) };
  }

  if (t2.tag === 'TVar') {
    if (occursIn(t2.name, t1)) return { ok: false, error: `Occurs check: ${t2.name} in ${t1}` };
    return { ok: true, subst: new Map([[t2.name, t1]]) };
  }

  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    const r1 = unify(t1.param, t2.param);
    if (!r1.ok) return r1;
    const r2 = unify(applySubst(r1.subst, t1.ret), applySubst(r1.subst, t2.ret));
    if (!r2.ok) return r2;
    return { ok: true, subst: composeSubst(r2.subst, r1.subst) };
  }

  if (t1.tag === 'TCon' && t2.tag === 'TCon') {
    if (t1.name !== t2.name || t1.args.length !== t2.args.length) {
      return { ok: false, error: `Cannot unify ${t1.name} with ${t2.name}` };
    }
    let subst = new Map();
    for (let i = 0; i < t1.args.length; i++) {
      const r = unify(applySubst(subst, t1.args[i]), applySubst(subst, t2.args[i]));
      if (!r.ok) return r;
      subst = composeSubst(r.subst, subst);
    }
    return { ok: true, subst };
  }

  return { ok: false, error: `Cannot unify ${t1} with ${t2}` };
}

function normalize(t) { return t; } // Placeholder for future normalization

// ============================================================
// Convenience
// ============================================================

function unifyAll(constraints) {
  let subst = new Map();
  for (const [t1, t2] of constraints) {
    const r = unify(applySubst(subst, t1), applySubst(subst, t2));
    if (!r.ok) return r;
    subst = composeSubst(r.subst, subst);
  }
  return { ok: true, subst };
}

function freeVars(type) {
  switch (type.tag) {
    case 'TVar': return new Set([type.name]);
    case 'TFun': return new Set([...freeVars(type.param), ...freeVars(type.ret)]);
    case 'TCon': return new Set(type.args.flatMap(a => [...freeVars(a)]));
    default: return new Set();
  }
}

export { TVar, TFun, TCon, applySubst, composeSubst, occursIn, unify, unifyAll, freeVars };
