/**
 * Bidirectional Type Checking
 * 
 * Two modes:
 * - infer(e) → T  (synthesize a type from the expression)
 * - check(e, T) → ok/error  (verify expression has given type)
 * 
 * Rules:
 *   infer(x)      = lookup(x)
 *   infer(e₁ e₂)  = infer(e₁) must be T₂→T, check(e₂, T₂), return T
 *   infer(n)      = Int
 *   check(λx.e, T₁→T₂) = extend x:T₁, check(e, T₂)
 *   check(e, T)   = infer(e) = T' and T' = T
 *   infer(e:T)    = check(e, T), return T  (annotation)
 * 
 * Advantage: fewer type annotations needed than STLC,
 * lambdas can be checked without annotating parameters.
 * 
 * Based on: Dunfield & Krishnaswami (2019)
 */

// ============================================================
// Types
// ============================================================

class TInt { constructor() { this.tag = 'TInt'; } toString() { return 'Int'; } }
class TBool { constructor() { this.tag = 'TBool'; } toString() { return 'Bool'; } }
class TStr { constructor() { this.tag = 'TStr'; } toString() { return 'Str'; } }
class TFun { constructor(param, ret) { this.tag = 'TFun'; this.param = param; this.ret = ret; } toString() { return `(${this.param} → ${this.ret})`; } }
class TUnit { constructor() { this.tag = 'TUnit'; } toString() { return 'Unit'; } }

const tInt = new TInt();
const tBool = new TBool();
const tStr = new TStr();
const tUnit = new TUnit();

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'TFun') return typeEquals(a.param, b.param) && typeEquals(a.ret, b.ret);
  return true;
}

// ============================================================
// Expressions
// ============================================================

class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class ELam { constructor(param, body) { this.tag = 'ELam'; this.param = param; this.body = body; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } }
class EBool { constructor(v) { this.tag = 'EBool'; this.v = v; } }
class EStr { constructor(s) { this.tag = 'EStr'; this.s = s; } }
class EAnn { constructor(expr, type) { this.tag = 'EAnn'; this.expr = expr; this.type = type; } }
class EIf { constructor(cond, then, els) { this.tag = 'EIf'; this.cond = cond; this.then = then; this.els = els; } }
class ELet { constructor(name, val, body) { this.tag = 'ELet'; this.name = name; this.val = val; this.body = body; } }

// ============================================================
// Bidirectional Type Checker
// ============================================================

class BidiChecker {
  constructor() {
    this.errors = [];
  }

  /**
   * Infer mode: synthesize a type from the expression
   */
  infer(expr, env = new Map()) {
    switch (expr.tag) {
      case 'ENum': return tInt;
      case 'EBool': return tBool;
      case 'EStr': return tStr;
      
      case 'EVar': {
        const type = env.get(expr.name);
        if (!type) {
          this.errors.push(`Unbound variable: ${expr.name}`);
          return tUnit;
        }
        return type;
      }
      
      case 'EApp': {
        const fnType = this.infer(expr.fn, env);
        if (fnType.tag !== 'TFun') {
          this.errors.push(`Expected function type, got ${fnType}`);
          return tUnit;
        }
        this.check(expr.arg, fnType.param, env);
        return fnType.ret;
      }
      
      case 'EAnn': {
        // Annotation: check expression against annotated type
        this.check(expr.expr, expr.type, env);
        return expr.type;
      }
      
      case 'ELam': {
        // Cannot infer lambda without annotation
        this.errors.push('Cannot infer type of unannotated lambda');
        return tUnit;
      }
      
      case 'EIf': {
        this.check(expr.cond, tBool, env);
        const thenType = this.infer(expr.then, env);
        this.check(expr.els, thenType, env);
        return thenType;
      }
      
      case 'ELet': {
        const valType = this.infer(expr.val, env);
        const newEnv = new Map(env);
        newEnv.set(expr.name, valType);
        return this.infer(expr.body, newEnv);
      }
      
      default:
        this.errors.push(`Cannot infer: ${expr.tag}`);
        return tUnit;
    }
  }

  /**
   * Check mode: verify expression has the given type
   */
  check(expr, type, env = new Map()) {
    switch (expr.tag) {
      case 'ELam': {
        // Lambda: check body against return type
        if (type.tag !== 'TFun') {
          this.errors.push(`Expected ${type}, but got lambda`);
          return;
        }
        const newEnv = new Map(env);
        newEnv.set(expr.param, type.param);
        this.check(expr.body, type.ret, newEnv);
        return;
      }
      
      case 'EIf': {
        this.check(expr.cond, tBool, env);
        this.check(expr.then, type, env);
        this.check(expr.els, type, env);
        return;
      }
      
      default: {
        // Subsumption: infer and compare
        const inferred = this.infer(expr, env);
        if (!typeEquals(inferred, type)) {
          this.errors.push(`Expected ${type}, got ${inferred}`);
        }
      }
    }
  }
}

/**
 * Convenience: infer type of expression
 */
function biInfer(expr, env) {
  const checker = new BidiChecker();
  const type = checker.infer(expr, env);
  return { type, errors: checker.errors };
}

/**
 * Convenience: check expression against type
 */
function biCheck(expr, type, env) {
  const checker = new BidiChecker();
  checker.check(expr, type, env);
  return { errors: checker.errors };
}

export {
  TInt, TBool, TStr, TFun, TUnit, tInt, tBool, tStr, tUnit, typeEquals,
  EVar, ELam, EApp, ENum, EBool, EStr, EAnn, EIf, ELet,
  BidiChecker, biInfer, biCheck
};
