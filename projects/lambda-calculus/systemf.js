/**
 * System F — Polymorphic Lambda Calculus (Second-Order)
 * 
 * Extends STLC with:
 * - Type variables (α, β, ...)
 * - Universal quantification (∀α. T)
 * - Type abstraction (Λα. t)
 * - Type application (t [T])
 * 
 * Key properties:
 * - Strongly normalizing (all well-typed terms terminate)
 * - Impredicative (∀ quantifies over ALL types, including polymorphic ones)
 * - Church encodings are typeable (unlike STLC)
 * - Parametricity: free theorems from types alone
 */

// ============================================================
// Types
// ============================================================

class TVar {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
  equals(other) { return other instanceof TVar && other.name === this.name; }
  freeVars() { return new Set([this.name]); }
  subst(name, type) { return this.name === name ? type : this; }
}

class TBool {
  toString() { return 'Bool'; }
  equals(other) { return other instanceof TBool; }
  freeVars() { return new Set(); }
  subst() { return this; }
}

class TInt {
  toString() { return 'Int'; }
  equals(other) { return other instanceof TInt; }
  freeVars() { return new Set(); }
  subst() { return this; }
}

class TUnit {
  toString() { return 'Unit'; }
  equals(other) { return other instanceof TUnit; }
  freeVars() { return new Set(); }
  subst() { return this; }
}

class TArrow {
  constructor(param, ret) { this.param = param; this.ret = ret; }
  toString() {
    const p = this.param instanceof TArrow || this.param instanceof TForall 
      ? `(${this.param})` : `${this.param}`;
    return `${p} → ${this.ret}`;
  }
  equals(other) {
    return other instanceof TArrow && this.param.equals(other.param) && this.ret.equals(other.ret);
  }
  freeVars() {
    const fv = this.param.freeVars();
    for (const v of this.ret.freeVars()) fv.add(v);
    return fv;
  }
  subst(name, type) {
    return new TArrow(this.param.subst(name, type), this.ret.subst(name, type));
  }
}

class TForall {
  constructor(typeVar, body) { this.typeVar = typeVar; this.body = body; }
  toString() { return `∀${this.typeVar}. ${this.body}`; }
  equals(other) {
    if (!(other instanceof TForall)) return false;
    // Alpha-equivalence: substitute both to a fresh variable
    if (this.typeVar === other.typeVar) return this.body.equals(other.body);
    // Check alpha-equivalence by substituting
    const fresh = `_α${Date.now()}`;
    const a = this.body.subst(this.typeVar, new TVar(fresh));
    const b = other.body.subst(other.typeVar, new TVar(fresh));
    return a.equals(b);
  }
  freeVars() {
    const fv = this.body.freeVars();
    fv.delete(this.typeVar);
    return fv;
  }
  subst(name, type) {
    if (name === this.typeVar) return this; // shadowed
    // Avoid capture
    const fv = type.freeVars();
    if (fv.has(this.typeVar)) {
      const fresh = this.typeVar + "'";
      const renamedBody = this.body.subst(this.typeVar, new TVar(fresh));
      return new TForall(fresh, renamedBody.subst(name, type));
    }
    return new TForall(this.typeVar, this.body.subst(name, type));
  }
}

class TProd {
  constructor(fst, snd) { this.fst = fst; this.snd = snd; }
  toString() { return `${this.fst} × ${this.snd}`; }
  equals(other) {
    return other instanceof TProd && this.fst.equals(other.fst) && this.snd.equals(other.snd);
  }
  freeVars() {
    const fv = this.fst.freeVars();
    for (const v of this.snd.freeVars()) fv.add(v);
    return fv;
  }
  subst(name, type) {
    return new TProd(this.fst.subst(name, type), this.snd.subst(name, type));
  }
}

// ============================================================
// Terms
// ============================================================

class FVar {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
}

class FAbs {
  constructor(param, paramType, body) {
    this.param = param; this.paramType = paramType; this.body = body;
  }
  toString() { return `(λ${this.param}:${this.paramType}. ${this.body})`; }
}

class FApp {
  constructor(func, arg) { this.func = func; this.arg = arg; }
  toString() { return `(${this.func} ${this.arg})`; }
}

class FTyAbs {
  constructor(typeVar, body) { this.typeVar = typeVar; this.body = body; }
  toString() { return `(Λ${this.typeVar}. ${this.body})`; }
}

class FTyApp {
  constructor(expr, type) { this.expr = expr; this.type = type; }
  toString() { return `(${this.expr} [${this.type}])`; }
}

class FBool {
  constructor(value) { this.value = value; }
  toString() { return String(this.value); }
}

class FInt {
  constructor(value) { this.value = value; }
  toString() { return String(this.value); }
}

class FUnit {
  toString() { return '()'; }
}

class FIf {
  constructor(cond, then, else_) { this.cond = cond; this.then = then; this.else_ = else_; }
  toString() { return `(if ${this.cond} then ${this.then} else ${this.else_})`; }
}

class FLet {
  constructor(name, value, body) { this.name = name; this.value = value; this.body = body; }
  toString() { return `(let ${this.name} = ${this.value} in ${this.body})`; }
}

class FBinOp {
  constructor(op, left, right) { this.op = op; this.left = left; this.right = right; }
  toString() { return `(${this.left} ${this.op} ${this.right})`; }
}

class FPair {
  constructor(fst, snd) { this.fst = fst; this.snd = snd; }
  toString() { return `(${this.fst}, ${this.snd})`; }
}

class FFst {
  constructor(pair) { this.pair = pair; }
  toString() { return `fst(${this.pair})`; }
}

class FSnd {
  constructor(pair) { this.pair = pair; }
  toString() { return `snd(${this.pair})`; }
}

// ============================================================
// Type Environment
// ============================================================

class TypeEnv {
  constructor(termBindings = new Map(), typeVars = new Set()) {
    this.termBindings = termBindings;
    this.typeVars = typeVars;
  }
  
  extendTerm(name, type) {
    const nb = new Map(this.termBindings);
    nb.set(name, type);
    return new TypeEnv(nb, new Set(this.typeVars));
  }
  
  extendType(typeVar) {
    const tv = new Set(this.typeVars);
    tv.add(typeVar);
    return new TypeEnv(new Map(this.termBindings), tv);
  }
  
  lookupTerm(name) {
    return this.termBindings.get(name) || null;
  }
  
  hasTypeVar(name) {
    return this.typeVars.has(name);
  }
}

// ============================================================
// Type Checker
// ============================================================

class FTypeError extends Error {
  constructor(msg) { super(msg); this.name = 'FTypeError'; }
}

function wellFormed(env, type) {
  if (type instanceof TVar) {
    if (!env.hasTypeVar(type.name)) throw new FTypeError(`Unbound type variable: ${type.name}`);
    return;
  }
  if (type instanceof TBool || type instanceof TInt || type instanceof TUnit) return;
  if (type instanceof TArrow) {
    wellFormed(env, type.param);
    wellFormed(env, type.ret);
    return;
  }
  if (type instanceof TForall) {
    wellFormed(env.extendType(type.typeVar), type.body);
    return;
  }
  if (type instanceof TProd) {
    wellFormed(env, type.fst);
    wellFormed(env, type.snd);
    return;
  }
  throw new FTypeError(`Unknown type: ${type}`);
}

function infer(env, term) {
  if (term instanceof FVar) {
    const t = env.lookupTerm(term.name);
    if (t === null) throw new FTypeError(`Unbound variable: ${term.name}`);
    return t;
  }
  
  if (term instanceof FBool) return new TBool();
  if (term instanceof FInt) return new TInt();
  if (term instanceof FUnit) return new TUnit();
  
  if (term instanceof FAbs) {
    wellFormed(env, term.paramType);
    const bodyType = infer(env.extendTerm(term.param, term.paramType), term.body);
    return new TArrow(term.paramType, bodyType);
  }
  
  if (term instanceof FApp) {
    const funcType = infer(env, term.func);
    if (!(funcType instanceof TArrow)) {
      throw new FTypeError(`Expected function type, got ${funcType}`);
    }
    check(env, term.arg, funcType.param);
    return funcType.ret;
  }
  
  // Type abstraction: Λα. t : ∀α. T
  if (term instanceof FTyAbs) {
    const bodyType = infer(env.extendType(term.typeVar), term.body);
    return new TForall(term.typeVar, bodyType);
  }
  
  // Type application: t [T] : T'[α := T]
  if (term instanceof FTyApp) {
    const exprType = infer(env, term.expr);
    if (!(exprType instanceof TForall)) {
      throw new FTypeError(`Expected ∀ type, got ${exprType}`);
    }
    wellFormed(env, term.type);
    return exprType.body.subst(exprType.typeVar, term.type);
  }
  
  if (term instanceof FIf) {
    check(env, term.cond, new TBool());
    const thenType = infer(env, term.then);
    check(env, term.else_, thenType);
    return thenType;
  }
  
  if (term instanceof FLet) {
    const valueType = infer(env, term.value);
    return infer(env.extendTerm(term.name, valueType), term.body);
  }
  
  if (term instanceof FBinOp) {
    const arithOps = ['+', '-', '*', '/', '%'];
    const cmpOps = ['<', '>', '<=', '>='];
    
    if (arithOps.includes(term.op)) {
      check(env, term.left, new TInt());
      check(env, term.right, new TInt());
      return new TInt();
    }
    if (cmpOps.includes(term.op)) {
      check(env, term.left, new TInt());
      check(env, term.right, new TInt());
      return new TBool();
    }
    if (term.op === '==' || term.op === '!=') {
      const lt = infer(env, term.left);
      check(env, term.right, lt);
      return new TBool();
    }
    if (term.op === '&&' || term.op === '||') {
      check(env, term.left, new TBool());
      check(env, term.right, new TBool());
      return new TBool();
    }
    throw new FTypeError(`Unknown operator: ${term.op}`);
  }
  
  if (term instanceof FPair) {
    return new TProd(infer(env, term.fst), infer(env, term.snd));
  }
  
  if (term instanceof FFst) {
    const pt = infer(env, term.pair);
    if (!(pt instanceof TProd)) throw new FTypeError(`Expected product, got ${pt}`);
    return pt.fst;
  }
  
  if (term instanceof FSnd) {
    const pt = infer(env, term.pair);
    if (!(pt instanceof TProd)) throw new FTypeError(`Expected product, got ${pt}`);
    return pt.snd;
  }
  
  throw new FTypeError(`Cannot infer type of: ${term}`);
}

function check(env, term, expected) {
  const actual = infer(env, term);
  if (!actual.equals(expected)) {
    throw new FTypeError(`Type mismatch: expected ${expected}, got ${actual}`);
  }
}

function typecheck(term) {
  return infer(new TypeEnv(), term);
}

// ============================================================
// Evaluator (call-by-value with type erasure)
// ============================================================

function isVal(term) {
  return term instanceof FBool || term instanceof FInt || term instanceof FUnit ||
         term instanceof FAbs || term instanceof FTyAbs || term instanceof FPair;
}

function substTerm(term, name, value) {
  if (term instanceof FVar) return term.name === name ? value : term;
  if (term instanceof FAbs) {
    if (term.param === name) return term;
    return new FAbs(term.param, term.paramType, substTerm(term.body, name, value));
  }
  if (term instanceof FApp) {
    return new FApp(substTerm(term.func, name, value), substTerm(term.arg, name, value));
  }
  if (term instanceof FTyAbs) {
    return new FTyAbs(term.typeVar, substTerm(term.body, name, value));
  }
  if (term instanceof FTyApp) {
    return new FTyApp(substTerm(term.expr, name, value), term.type);
  }
  if (term instanceof FBool || term instanceof FInt || term instanceof FUnit) return term;
  if (term instanceof FIf) {
    return new FIf(substTerm(term.cond, name, value),
      substTerm(term.then, name, value), substTerm(term.else_, name, value));
  }
  if (term instanceof FLet) {
    const nv = substTerm(term.value, name, value);
    if (term.name === name) return new FLet(term.name, nv, term.body);
    return new FLet(term.name, nv, substTerm(term.body, name, value));
  }
  if (term instanceof FBinOp) {
    return new FBinOp(term.op, substTerm(term.left, name, value), substTerm(term.right, name, value));
  }
  if (term instanceof FPair) {
    return new FPair(substTerm(term.fst, name, value), substTerm(term.snd, name, value));
  }
  if (term instanceof FFst) return new FFst(substTerm(term.pair, name, value));
  if (term instanceof FSnd) return new FSnd(substTerm(term.pair, name, value));
  return term;
}

function evalStep(term) {
  if (term instanceof FApp) {
    if (!isVal(term.func)) {
      const f = evalStep(term.func);
      return f !== null ? new FApp(f, term.arg) : null;
    }
    if (!isVal(term.arg)) {
      const a = evalStep(term.arg);
      return a !== null ? new FApp(term.func, a) : null;
    }
    if (term.func instanceof FAbs) {
      return substTerm(term.func.body, term.func.param, term.arg);
    }
    return null;
  }
  
  if (term instanceof FTyApp) {
    if (!isVal(term.expr)) {
      const e = evalStep(term.expr);
      return e !== null ? new FTyApp(e, term.type) : null;
    }
    if (term.expr instanceof FTyAbs) {
      // Type erasure: just drop the type application and return the body
      return term.expr.body;
    }
    return null;
  }
  
  if (term instanceof FIf) {
    if (!isVal(term.cond)) {
      const c = evalStep(term.cond);
      return c !== null ? new FIf(c, term.then, term.else_) : null;
    }
    if (term.cond instanceof FBool) {
      return term.cond.value ? term.then : term.else_;
    }
    return null;
  }
  
  if (term instanceof FLet) {
    if (!isVal(term.value)) {
      const v = evalStep(term.value);
      return v !== null ? new FLet(term.name, v, term.body) : null;
    }
    return substTerm(term.body, term.name, term.value);
  }
  
  if (term instanceof FBinOp) {
    if (!isVal(term.left)) {
      const l = evalStep(term.left);
      return l !== null ? new FBinOp(term.op, l, term.right) : null;
    }
    if (!isVal(term.right)) {
      const r = evalStep(term.right);
      return r !== null ? new FBinOp(term.op, term.left, r) : null;
    }
    if (term.left instanceof FInt && term.right instanceof FInt) {
      const l = term.left.value, r = term.right.value;
      switch (term.op) {
        case '+': return new FInt(l + r);
        case '-': return new FInt(l - r);
        case '*': return new FInt(l * r);
        case '/': return new FInt(Math.trunc(l / r));
        case '%': return new FInt(l % r);
        case '<': return new FBool(l < r);
        case '>': return new FBool(l > r);
        case '<=': return new FBool(l <= r);
        case '>=': return new FBool(l >= r);
        case '==': return new FBool(l === r);
        case '!=': return new FBool(l !== r);
      }
    }
    if (term.left instanceof FBool && term.right instanceof FBool) {
      switch (term.op) {
        case '&&': return new FBool(term.left.value && term.right.value);
        case '||': return new FBool(term.left.value || term.right.value);
        case '==': return new FBool(term.left.value === term.right.value);
        case '!=': return new FBool(term.left.value !== term.right.value);
      }
    }
    return null;
  }
  
  if (term instanceof FPair) {
    if (!isVal(term.fst)) {
      const f = evalStep(term.fst);
      return f !== null ? new FPair(f, term.snd) : null;
    }
    if (!isVal(term.snd)) {
      const s = evalStep(term.snd);
      return s !== null ? new FPair(term.fst, s) : null;
    }
    return null;
  }
  
  if (term instanceof FFst) {
    if (!isVal(term.pair)) {
      const p = evalStep(term.pair);
      return p !== null ? new FFst(p) : null;
    }
    if (term.pair instanceof FPair) return term.pair.fst;
    return null;
  }
  
  if (term instanceof FSnd) {
    if (!isVal(term.pair)) {
      const p = evalStep(term.pair);
      return p !== null ? new FSnd(p) : null;
    }
    if (term.pair instanceof FPair) return term.pair.snd;
    return null;
  }
  
  return null;
}

function evaluate(term, maxSteps = 10000) {
  let current = term;
  let steps = 0;
  
  while (steps < maxSteps) {
    const next = evalStep(current);
    if (next === null) break;
    current = next;
    steps++;
  }
  
  return { result: current, steps, normalForm: steps < maxSteps };
}

// ============================================================
// Exports
// ============================================================

export {
  // Types
  TVar, TBool, TInt, TUnit, TArrow, TForall, TProd,
  // Terms
  FVar, FAbs, FApp, FTyAbs, FTyApp,
  FBool, FInt, FUnit, FIf, FLet, FBinOp, FPair, FFst, FSnd,
  // Type checking
  TypeEnv, FTypeError, typecheck, infer, check, wellFormed,
  // Evaluation
  evaluate, evalStep, isVal,
};
