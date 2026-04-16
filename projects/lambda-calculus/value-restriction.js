/**
 * Value Restriction (SML/OCaml)
 * 
 * Problem: if we generalize non-values (like function applications),
 * we can create type-unsound programs with mutable references.
 * 
 * let r = ref [] in r := [1]; head(!r) + "hello"
 *   — Without restriction: r : ∀a. ref [a]  (UNSOUND!)
 *   — With restriction: r : ref [_weak1]  (monomorphic)
 * 
 * Value restriction: only generalize let-bindings where the RHS is
 * a syntactic value (variable, lambda, literal, constructor).
 */

// Simple expression classifier
function isSyntacticValue(expr) {
  switch (expr.tag) {
    case 'EVar': case 'ENum': case 'EBool': case 'EStr':
      return true;
    case 'ELam':
      return true; // Lambda is always a value
    case 'ECon':
      return (expr.args || []).every(isSyntacticValue);
    case 'ETuple':
      return (expr.elements || []).every(isSyntacticValue);
    default:
      return false; // Function applications, etc. are NOT values
  }
}

// Expression types
class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } }
class EBool { constructor(v) { this.tag = 'EBool'; this.v = v; } }
class EStr { constructor(s) { this.tag = 'EStr'; this.s = s; } }
class ELam { constructor(p, b) { this.tag = 'ELam'; this.param = p; this.body = b; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class ELet { constructor(name, val, body) { this.tag = 'ELet'; this.name = name; this.val = val; this.body = body; } }
class ERef { constructor(val) { this.tag = 'ERef'; this.val = val; } }
class ECon { constructor(name, args) { this.tag = 'ECon'; this.name = name; this.args = args; } }
class ETuple { constructor(elements) { this.tag = 'ETuple'; this.elements = elements; } }

// Type types
class TVar { constructor(name, weak = false) { this.tag = 'TVar'; this.name = name; this.weak = weak; } toString() { return this.weak ? `_${this.name}` : this.name; } }
class TCon { constructor(name) { this.tag = 'TCon'; this.name = name; } toString() { return this.name; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TRef { constructor(inner) { this.tag = 'TRef'; this.inner = inner; } toString() { return `ref ${this.inner}`; } }
class Scheme { constructor(vars, type) { this.vars = vars; this.type = type; } toString() { return this.vars.length > 0 ? `∀${this.vars.join(' ')}.${this.type}` : `${this.type}`; } }

const tInt = new TCon('Int');
const tBool = new TCon('Bool');
const tStr = new TCon('Str');

// Value-restricted generalization
function generalizeRestricted(env, type, isValue) {
  if (!isValue) {
    // Non-value: create weak/monomorphic type variables (no generalization)
    return new Scheme([], type);
  }
  // Value: normal generalization
  const envFtv = ftvEnv(env);
  const typeFtv = ftv(type);
  const vars = [...typeFtv].filter(v => !envFtv.has(v));
  return new Scheme(vars, type);
}

function ftv(type) {
  switch (type.tag) {
    case 'TVar': return new Set([type.name]);
    case 'TFun': return new Set([...ftv(type.param), ...ftv(type.ret)]);
    case 'TRef': return ftv(type.inner);
    case 'TCon': return new Set();
    default: return new Set();
  }
}

function ftvEnv(env) {
  const result = new Set();
  for (const [, scheme] of env) {
    const bodyFtv = ftv(scheme.type);
    for (const v of scheme.vars) bodyFtv.delete(v);
    for (const v of bodyFtv) result.add(v);
  }
  return result;
}

export {
  isSyntacticValue,
  EVar, ENum, EBool, EStr, ELam, EApp, ELet, ERef, ECon, ETuple,
  TVar, TCon, TFun, TRef, Scheme,
  tInt, tBool, tStr,
  generalizeRestricted, ftv, ftvEnv
};
