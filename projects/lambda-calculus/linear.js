/**
 * Linear Type System
 * 
 * A type system with substructural typing:
 * - Linear: use exactly once (no weakening, no contraction)
 * - Affine: use at most once (weakening ok, no contraction) — Rust's model
 * - Relevant: use at least once (no weakening, contraction ok)
 * - Unrestricted: use any number of times (normal types)
 * 
 * Linear types ensure resources are properly managed:
 * - File handles must be closed
 * - Memory must be freed
 * - Channels must be used
 * 
 * Based on:
 * - Girard's Linear Logic (1987)
 * - Wadler's "Linear types can change the world" (1990)
 * - Rust's ownership and borrowing (practical application)
 */

// ============================================================
// Types
// ============================================================

// Usage modality
const LINEAR = 'linear';        // Exactly once
const AFFINE = 'affine';        // At most once (Rust-like)
const RELEVANT = 'relevant';    // At least once
const UNRESTRICTED = 'unrestricted'; // Any number of times

class LType {
  constructor(base, modality = UNRESTRICTED) {
    this.base = base;     // Base type (string like 'Int', 'Bool', 'String', or complex type)
    this.modality = modality;
  }
  toString() {
    const prefix = {
      [LINEAR]: '!', [AFFINE]: '?', [RELEVANT]: '+', [UNRESTRICTED]: ''
    }[this.modality];
    return `${prefix}${this.base}`;
  }
}

class LFun {
  constructor(param, paramType, ret, retEffects = null) {
    this.tag = 'LFun';
    this.param = param;
    this.paramType = paramType;
    this.ret = ret;
  }
  toString() { return `(${this.paramType} ⊸ ${this.ret})`; }
}

class LPair {
  constructor(fst, snd) { this.tag = 'LPair'; this.fst = fst; this.snd = snd; }
  toString() { return `(${this.fst} ⊗ ${this.snd})`; }
}

// Resource types
class LResource {
  constructor(name, modality = LINEAR) {
    this.tag = 'LResource';
    this.name = name;
    this.modality = modality;
  }
  toString() { return `Resource<${this.name}>`; }
}

// ============================================================
// AST (minimal linear lambda calculus)
// ============================================================

class LVar { constructor(name) { this.tag = 'LVar'; this.name = name; } }
class LLam { constructor(param, paramType, body) { this.tag = 'LLam'; this.param = param; this.paramType = paramType; this.body = body; } }
class LApp { constructor(fn, arg) { this.tag = 'LApp'; this.fn = fn; this.arg = arg; } }
class LLet { constructor(name, value, body) { this.tag = 'LLet'; this.name = name; this.value = value; this.body = body; } }
class LPairExpr { constructor(fst, snd) { this.tag = 'LPairExpr'; this.fst = fst; this.snd = snd; } }
class LLetPair { constructor(fst, snd, pair, body) { this.tag = 'LLetPair'; this.fst = fst; this.snd = snd; this.pair = pair; this.body = body; } }
class LLit { constructor(value, type) { this.tag = 'LLit'; this.value = value; this.type = type; } }

// Resource operations
class LNew { constructor(resourceType) { this.tag = 'LNew'; this.resourceType = resourceType; } }
class LUse { constructor(resource, body) { this.tag = 'LUse'; this.resource = resource; this.body = body; } }
class LClose { constructor(resource) { this.tag = 'LClose'; this.resource = resource; } }

// ============================================================
// Usage Tracking
// ============================================================

class UsageMap {
  constructor(parent = null) {
    this.uses = new Map(); // name → count
    this.types = new Map(); // name → LType
    this.parent = parent;
  }
  
  bind(name, type) {
    this.types.set(name, type);
    this.uses.set(name, 0);
  }
  
  use(name) {
    if (this.uses.has(name)) {
      this.uses.set(name, this.uses.get(name) + 1);
      return this.types.get(name);
    }
    if (this.parent) return this.parent.use(name);
    return null;
  }
  
  lookup(name) {
    if (this.types.has(name)) return this.types.get(name);
    if (this.parent) return this.parent.lookup(name);
    return null;
  }
  
  getUsage(name) {
    if (this.uses.has(name)) return this.uses.get(name);
    if (this.parent) return this.parent.getUsage(name);
    return 0;
  }
  
  extend() {
    return new UsageMap(this);
  }
  
  // Check linearity constraints for all bindings in this scope
  checkConstraints() {
    const errors = [];
    for (const [name, type] of this.types) {
      const count = this.uses.get(name) || 0;
      const modality = type.modality || UNRESTRICTED;
      
      switch (modality) {
        case LINEAR:
          if (count !== 1) {
            errors.push(`Linear variable '${name}' used ${count} time(s), must be exactly 1`);
          }
          break;
        case AFFINE:
          if (count > 1) {
            errors.push(`Affine variable '${name}' used ${count} time(s), must be at most 1`);
          }
          break;
        case RELEVANT:
          if (count < 1) {
            errors.push(`Relevant variable '${name}' never used, must be at least 1`);
          }
          break;
        // UNRESTRICTED: no constraint
      }
    }
    return errors;
  }
}

// ============================================================
// Linear Type Checker
// ============================================================

class LinearTypeError extends Error {
  constructor(msg) { super(msg); this.name = 'LinearTypeError'; }
}

function linearCheck(expr, usage = new UsageMap()) {
  const result = { type: null, errors: [] };
  
  function check(expr, usg) {
    switch (expr.tag) {
      case 'LLit':
        return expr.type;
        
      case 'LVar': {
        const type = usg.use(expr.name);
        if (!type) {
          result.errors.push(`Unbound variable: ${expr.name}`);
          return new LType('unknown');
        }
        return type;
      }
      
      case 'LLam': {
        const bodyUsg = usg.extend();
        bodyUsg.bind(expr.param, expr.paramType);
        const retType = check(expr.body, bodyUsg);
        
        // Check linearity of the parameter
        const paramErrors = bodyUsg.checkConstraints();
        result.errors.push(...paramErrors);
        
        return new LFun(expr.param, expr.paramType, retType);
      }
      
      case 'LApp': {
        const fnType = check(expr.fn, usg);
        const argType = check(expr.arg, usg);
        
        if (fnType instanceof LFun) {
          return fnType.ret;
        }
        return new LType('unknown');
      }
      
      case 'LLet': {
        const valType = check(expr.value, usg);
        const bodyUsg = usg.extend();
        // Inherit modality from value type or default to unrestricted
        bodyUsg.bind(expr.name, valType);
        const retType = check(expr.body, bodyUsg);
        
        const letErrors = bodyUsg.checkConstraints();
        result.errors.push(...letErrors);
        
        return retType;
      }
      
      case 'LPairExpr': {
        const fstType = check(expr.fst, usg);
        const sndType = check(expr.snd, usg);
        return new LPair(fstType, sndType);
      }
      
      case 'LLetPair': {
        const pairType = check(expr.pair, usg);
        const bodyUsg = usg.extend();
        
        if (pairType instanceof LPair) {
          bodyUsg.bind(expr.fst, pairType.fst);
          bodyUsg.bind(expr.snd, pairType.snd);
        } else {
          bodyUsg.bind(expr.fst, new LType('unknown'));
          bodyUsg.bind(expr.snd, new LType('unknown'));
        }
        
        const retType = check(expr.body, bodyUsg);
        const pairErrors = bodyUsg.checkConstraints();
        result.errors.push(...pairErrors);
        
        return retType;
      }
      
      case 'LNew': {
        return new LResource(expr.resourceType, LINEAR);
      }
      
      case 'LUse': {
        const resType = check(expr.resource, usg);
        const bodyType = check(expr.body, usg);
        return bodyType;
      }
      
      case 'LClose': {
        const resType = check(expr.resource, usg);
        return new LType('Unit');
      }
      
      default:
        return new LType('unknown');
    }
  }
  
  result.type = check(expr, usage);
  
  // Check top-level constraints
  result.errors.push(...usage.checkConstraints());
  
  return result;
}

// ============================================================
// Convenience constructors
// ============================================================

function lvar(name) { return new LVar(name); }
function llam(param, type, body) { return new LLam(param, type, body); }
function lapp(fn, arg) { return new LApp(fn, arg); }
function llet(name, value, body) { return new LLet(name, value, body); }
function lint(n) { return new LLit(n, new LType('Int')); }
function lbool(b) { return new LLit(b, new LType('Bool')); }
function lstr(s) { return new LLit(s, new LType('String')); }
function lunit() { return new LLit(null, new LType('Unit')); }
function lpair(a, b) { return new LPairExpr(a, b); }
function lletpair(a, b, pair, body) { return new LLetPair(a, b, pair, body); }
function lnew(type) { return new LNew(type); }
function luse(res, body) { return new LUse(res, body); }
function lclose(res) { return new LClose(res); }

// Type constructors with modality
function linear(base) { return new LType(base, LINEAR); }
function affine(base) { return new LType(base, AFFINE); }
function relevant(base) { return new LType(base, RELEVANT); }
function unrestricted(base) { return new LType(base, UNRESTRICTED); }

// ============================================================
// Exports
// ============================================================

export {
  LType, LFun, LPair, LResource,
  LINEAR, AFFINE, RELEVANT, UNRESTRICTED,
  LVar, LLam, LApp, LLet, LPairExpr, LLetPair, LLit, LNew, LUse, LClose,
  UsageMap, linearCheck, LinearTypeError,
  lvar, llam, lapp, llet, lint, lbool, lstr, lunit, lpair, lletpair, lnew, luse, lclose,
  linear, affine, relevant, unrestricted
};
