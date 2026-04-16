/**
 * Kind Inference
 * 
 * Kinds classify types, just as types classify values.
 * - Int : *             (concrete type, kind "star")
 * - List : * → *        (takes a type, returns a type)
 * - Map : * → * → *     (takes two types)
 * - Functor : (* → *) → Constraint  (takes a type constructor)
 * 
 * Kind inference: given type expressions, determine their kinds.
 */

// Kinds
class KStar { constructor() { this.tag = 'KStar'; } toString() { return '*'; } }
class KArrow { constructor(p, r) { this.tag = 'KArrow'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class KVar { constructor(name) { this.tag = 'KVar'; this.name = name; this.ref = null; } toString() { return this.ref ? this.ref.toString() : `?k${this.name}`; } }

const star = new KStar();

// Types (with kind annotations)
class TCon { constructor(name) { this.tag = 'TCon'; this.name = name; } }
class TApp { constructor(fn, arg) { this.tag = 'TApp'; this.fn = fn; this.arg = arg; } }
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } }
class TArrow { constructor(p, r) { this.tag = 'TArrow'; this.param = p; this.ret = r; } }

// ============================================================
// Kind inference
// ============================================================

let kindVarCounter = 0;
function freshKindVar() { return new KVar(kindVarCounter++); }
function resetKindVars() { kindVarCounter = 0; }

function resolveKind(k) {
  if (k.tag === 'KVar' && k.ref) return resolveKind(k.ref);
  return k;
}

function unifyKinds(k1, k2) {
  k1 = resolveKind(k1);
  k2 = resolveKind(k2);
  
  if (k1.tag === 'KStar' && k2.tag === 'KStar') return true;
  if (k1.tag === 'KVar') { k1.ref = k2; return true; }
  if (k2.tag === 'KVar') { k2.ref = k1; return true; }
  if (k1.tag === 'KArrow' && k2.tag === 'KArrow') {
    return unifyKinds(k1.param, k2.param) && unifyKinds(k1.ret, k2.ret);
  }
  return false;
}

class KindChecker {
  constructor() {
    this.env = new Map(); // type name → kind
    this.errors = [];
  }

  addPrimitive(name, kind) {
    this.env.set(name, kind);
  }

  infer(type) {
    switch (type.tag) {
      case 'TCon': {
        const kind = this.env.get(type.name);
        if (!kind) {
          this.errors.push(`Unknown type constructor: ${type.name}`);
          return freshKindVar();
        }
        return kind;
      }
      
      case 'TVar': {
        if (!this.env.has(type.name)) {
          const k = freshKindVar();
          this.env.set(type.name, k);
        }
        return this.env.get(type.name);
      }
      
      case 'TApp': {
        const fnKind = this.infer(type.fn);
        const argKind = this.infer(type.arg);
        const resultKind = freshKindVar();
        if (!unifyKinds(fnKind, new KArrow(argKind, resultKind))) {
          this.errors.push(`Kind error: ${type.fn.name || 'expr'} applied to wrong kind`);
        }
        return resultKind;
      }
      
      case 'TArrow': {
        const paramKind = this.infer(type.param);
        const retKind = this.infer(type.ret);
        if (!unifyKinds(paramKind, star)) this.errors.push('Arrow param must have kind *');
        if (!unifyKinds(retKind, star)) this.errors.push('Arrow return must have kind *');
        return star;
      }
      
      default:
        return star;
    }
  }

  check(type, expectedKind) {
    const inferred = this.infer(type);
    if (!unifyKinds(inferred, expectedKind)) {
      this.errors.push(`Expected kind ${expectedKind}, got ${inferred}`);
      return false;
    }
    return true;
  }
}

function createStdEnv() {
  const kc = new KindChecker();
  kc.addPrimitive('Int', star);
  kc.addPrimitive('Bool', star);
  kc.addPrimitive('String', star);
  kc.addPrimitive('List', new KArrow(star, star));
  kc.addPrimitive('Maybe', new KArrow(star, star));
  kc.addPrimitive('Map', new KArrow(star, new KArrow(star, star)));
  kc.addPrimitive('Either', new KArrow(star, new KArrow(star, star)));
  return kc;
}

export {
  KStar, KArrow, KVar, star,
  TCon, TApp, TVar, TArrow,
  freshKindVar, resetKindVars, resolveKind, unifyKinds,
  KindChecker, createStdEnv
};
