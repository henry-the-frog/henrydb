/**
 * Monomorphization: Type-Directed Specialization
 * 
 * Transform polymorphic programs into monomorphic ones by creating
 * specialized copies of functions for each type they're used at.
 * 
 * Before: let id = λx. x in (id 42, id "hello")
 * After:  let id_int = λx. x in let id_str = λx. x in (id_int 42, id_str "hello")
 * 
 * This is how Rust, C++ templates, and MLton compile polymorphism.
 * Trade-off: code size for performance (no runtime type dispatch).
 */

// ============================================================
// Types
// ============================================================

class TInt { constructor() { this.tag = 'TInt'; } toString() { return 'Int'; } }
class TBool { constructor() { this.tag = 'TBool'; } toString() { return 'Bool'; } }
class TStr { constructor() { this.tag = 'TStr'; } toString() { return 'Str'; } }
class TFun { constructor(param, ret) { this.tag = 'TFun'; this.param = param; this.ret = ret; } toString() { return `(${this.param} → ${this.ret})`; } }
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }

const tInt = new TInt();
const tBool = new TBool();
const tStr = new TStr();

function typeKey(type) {
  switch (type.tag) {
    case 'TInt': return 'Int';
    case 'TBool': return 'Bool';
    case 'TStr': return 'Str';
    case 'TFun': return `(${typeKey(type.param)}->${typeKey(type.ret)})`;
    case 'TVar': return type.name;
    default: return '?';
  }
}

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'TFun') return typeEquals(a.param, b.param) && typeEquals(a.ret, b.ret);
  if (a.tag === 'TVar') return a.name === b.name;
  return true;
}

// ============================================================
// Expressions
// ============================================================

class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } toString() { return this.name; } }
class ELam { constructor(param, body) { this.tag = 'ELam'; this.param = param; this.body = body; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } toString() { return `${this.n}`; } }
class EStr { constructor(s) { this.tag = 'EStr'; this.s = s; } toString() { return `"${this.s}"`; } }
class EBool { constructor(v) { this.tag = 'EBool'; this.v = v; } toString() { return `${this.v}`; } }
class ELet { constructor(name, val, body) { this.tag = 'ELet'; this.name = name; this.val = val; this.body = body; } }

// ============================================================
// Monomorphization
// ============================================================

class Monomorphizer {
  constructor() {
    this.specializations = new Map(); // "funcName:typeKey" → specialized name
    this.generated = [];              // Generated specialized functions
  }

  /**
   * Monomorphize a program given type information.
   * typeInfo: Map<exprId, type> — types of applications
   */
  monomorphize(expr, callTypes = new Map()) {
    this._collectSpecializations(expr, callTypes);
    const result = this._transform(expr, callTypes);
    return {
      program: result,
      specializations: [...this.specializations.entries()].map(([key, name]) => ({ key, name })),
      generatedCount: this.specializations.size
    };
  }

  _collectSpecializations(expr, callTypes) {
    if (expr.tag === 'EApp' && expr.fn.tag === 'EVar') {
      const argType = callTypes.get(expr);
      if (argType) {
        const key = `${expr.fn.name}:${typeKey(argType)}`;
        if (!this.specializations.has(key)) {
          this.specializations.set(key, `${expr.fn.name}_${typeKey(argType).replace(/[^a-zA-Z0-9]/g, '_')}`);
        }
      }
    }
    
    // Recurse
    if (expr.tag === 'EApp') {
      this._collectSpecializations(expr.fn, callTypes);
      this._collectSpecializations(expr.arg, callTypes);
    }
    if (expr.tag === 'ELet') {
      this._collectSpecializations(expr.val, callTypes);
      this._collectSpecializations(expr.body, callTypes);
    }
    if (expr.tag === 'ELam') {
      this._collectSpecializations(expr.body, callTypes);
    }
  }

  _transform(expr, callTypes) {
    switch (expr.tag) {
      case 'ENum': case 'EStr': case 'EBool': return expr;
      case 'EVar': return expr;
      
      case 'EApp': {
        if (expr.fn.tag === 'EVar') {
          const argType = callTypes.get(expr);
          if (argType) {
            const key = `${expr.fn.name}:${typeKey(argType)}`;
            const specialName = this.specializations.get(key);
            if (specialName) {
              return new EApp(new EVar(specialName), this._transform(expr.arg, callTypes));
            }
          }
        }
        return new EApp(this._transform(expr.fn, callTypes), this._transform(expr.arg, callTypes));
      }
      
      case 'ELam': return new ELam(expr.param, this._transform(expr.body, callTypes));
      
      case 'ELet': return new ELet(expr.name, this._transform(expr.val, callTypes), this._transform(expr.body, callTypes));
      
      default: return expr;
    }
  }
}

/**
 * Simple monomorphization: given a polymorphic function and its call sites,
 * produce specialized versions.
 */
function specialize(funcName, funcBody, callSites) {
  const results = [];
  
  for (const { argType, name: specName } of callSites) {
    results.push({
      name: specName || `${funcName}_${typeKey(argType)}`,
      argType,
      body: funcBody // In practice, would substitute type vars
    });
  }
  
  return results;
}

export {
  TInt, TBool, TStr, TFun, TVar, tInt, tBool, tStr,
  EVar, ELam, EApp, ENum, EStr, EBool, ELet,
  Monomorphizer, specialize, typeKey, typeEquals
};
