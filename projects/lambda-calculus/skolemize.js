/**
 * Skolemization: Convert ∀-bound types to rigid type constants
 * 
 * When checking: ∀a. F(a), Skolemize a → sk₀ (fresh rigid constant).
 * If F(sk₀) holds, then ∀a.F(a) holds (since sk₀ was arbitrary).
 * 
 * Uses:
 * 1. Higher-rank polymorphism: checking ∀ types during subsumption
 * 2. GADTs: rigid type variables in pattern matching
 * 3. Type checking: ensure types don't escape their scope
 */

// Types
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TSkolem { constructor(name, id) { this.tag = 'TSkolem'; this.name = name; this.id = id; } toString() { return `sk_${this.name}${this.id}`; } }
class TForall { constructor(v, body) { this.tag = 'TForall'; this.var = v; this.body = body; } toString() { return `∀${this.var}.${this.body}`; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TMeta { constructor(id) { this.tag = 'TMeta'; this.id = id; this.ref = null; } toString() { return this.ref ? this.ref.toString() : `?${this.id}`; } }

const tInt = new TBase('Int');
const tBool = new TBase('Bool');

let skolemCounter = 0;
let metaCounter = 0;
function freshSkolem(name) { return new TSkolem(name, skolemCounter++); }
function freshMeta() { return new TMeta(metaCounter++); }
function resetCounters() { skolemCounter = 0; metaCounter = 0; }

// ============================================================
// Skolemization
// ============================================================

function skolemize(type) {
  if (type.tag !== 'TForall') return { type, skolems: [] };
  
  const skolems = [];
  let current = type;
  
  while (current.tag === 'TForall') {
    const sk = freshSkolem(current.var);
    skolems.push(sk);
    current = substitute(current.body, current.var, sk);
  }
  
  return { type: current, skolems };
}

function substitute(type, varName, replacement) {
  switch (type.tag) {
    case 'TVar': return type.name === varName ? replacement : type;
    case 'TForall': return type.var === varName ? type : new TForall(type.var, substitute(type.body, varName, replacement));
    case 'TFun': return new TFun(substitute(type.param, varName, replacement), substitute(type.ret, varName, replacement));
    case 'TSkolem': return type;
    case 'TBase': return type;
    case 'TMeta': return type.ref ? substitute(type.ref, varName, replacement) : type;
    default: return type;
  }
}

// ============================================================
// Subsumption checking (with Skolemization)
// ============================================================

function subsumes(t1, t2) {
  // t1 subsumes t2 means: t1 is at least as polymorphic as t2
  // ∀a.a→a subsumes Int→Int (instantiate a=Int)
  
  // Skolemize t2's ∀s
  const { type: sk2, skolems } = skolemize(t2);
  
  // Instantiate t1's ∀s with fresh metas
  const inst1 = instantiate(t1);
  
  // Unify
  const result = unify(inst1, sk2);
  
  return result;
}

function instantiate(type) {
  if (type.tag !== 'TForall') return type;
  let current = type;
  while (current.tag === 'TForall') {
    const meta = freshMeta();
    current = substitute(current.body, current.var, meta);
  }
  return current;
}

function unify(t1, t2) {
  t1 = resolve(t1);
  t2 = resolve(t2);
  
  if (t1.tag === 'TMeta') { t1.ref = t2; return true; }
  if (t2.tag === 'TMeta') { t2.ref = t1; return true; }
  if (t1.tag === 'TBase' && t2.tag === 'TBase') return t1.name === t2.name;
  if (t1.tag === 'TSkolem' && t2.tag === 'TSkolem') return t1.id === t2.id;
  if (t1.tag === 'TFun' && t2.tag === 'TFun') return unify(t1.param, t2.param) && unify(t1.ret, t2.ret);
  return false;
}

function resolve(t) {
  if (t.tag === 'TMeta' && t.ref) return resolve(t.ref);
  return t;
}

function escapes(skolem, type) {
  type = resolve(type);
  if (type.tag === 'TSkolem') return type.id === skolem.id;
  if (type.tag === 'TFun') return escapes(skolem, type.param) || escapes(skolem, type.ret);
  if (type.tag === 'TMeta' && type.ref) return escapes(skolem, type.ref);
  return false;
}

// ============================================================
// Free variables
// ============================================================

function freeVars(type) {
  switch (type.tag) {
    case 'TVar': return new Set([type.name]);
    case 'TForall': { const fv = freeVars(type.body); fv.delete(type.var); return fv; }
    case 'TFun': return new Set([...freeVars(type.param), ...freeVars(type.ret)]);
    default: return new Set();
  }
}

export {
  TVar, TSkolem, TForall, TFun, TBase, TMeta,
  tInt, tBool,
  freshSkolem, freshMeta, resetCounters,
  skolemize, substitute, subsumes, instantiate,
  unify, resolve, escapes, freeVars
};
