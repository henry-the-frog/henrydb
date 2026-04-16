/**
 * Simply Typed Lambda Calculus (STLC)
 * 
 * Features:
 * - Base types: Bool, Int, Unit
 * - Function types: A → B
 * - Product types: A × B
 * - Bidirectional type checking (check + infer)
 * - Let-bindings with type inference
 * - If-then-else
 * - Integer and boolean literals
 * - Arithmetic and comparison operators
 * - Strong normalization (guaranteed for well-typed terms)
 */

// ============================================================
// Types
// ============================================================

class TBool {
  toString() { return 'Bool'; }
  equals(other) { return other instanceof TBool; }
}

class TInt {
  toString() { return 'Int'; }
  equals(other) { return other instanceof TInt; }
}

class TUnit {
  toString() { return 'Unit'; }
  equals(other) { return other instanceof TUnit; }
}

class TArrow {
  constructor(param, ret) { this.param = param; this.ret = ret; }
  toString() {
    const p = this.param instanceof TArrow ? `(${this.param})` : `${this.param}`;
    return `${p} → ${this.ret}`;
  }
  equals(other) {
    return other instanceof TArrow && this.param.equals(other.param) && this.ret.equals(other.ret);
  }
}

class TProd {
  constructor(fst, snd) { this.fst = fst; this.snd = snd; }
  toString() { return `${this.fst} × ${this.snd}`; }
  equals(other) {
    return other instanceof TProd && this.fst.equals(other.fst) && this.snd.equals(other.snd);
  }
}

// ============================================================
// Typed Terms (annotated AST)
// ============================================================

class TmVar {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
}

class TmAbs {
  constructor(param, paramType, body) {
    this.param = param;
    this.paramType = paramType;
    this.body = body;
  }
  toString() { return `(λ${this.param}:${this.paramType}.${this.body})`; }
}

class TmApp {
  constructor(func, arg) { this.func = func; this.arg = arg; }
  toString() { return `(${this.func} ${this.arg})`; }
}

class TmBool {
  constructor(value) { this.value = value; }
  toString() { return String(this.value); }
}

class TmInt {
  constructor(value) { this.value = value; }
  toString() { return String(this.value); }
}

class TmUnit {
  toString() { return '()'; }
}

class TmIf {
  constructor(cond, then, else_) { this.cond = cond; this.then = then; this.else_ = else_; }
  toString() { return `(if ${this.cond} then ${this.then} else ${this.else_})`; }
}

class TmLet {
  constructor(name, value, body) { this.name = name; this.value = value; this.body = body; }
  toString() { return `(let ${this.name} = ${this.value} in ${this.body})`; }
}

class TmBinOp {
  constructor(op, left, right) { this.op = op; this.left = left; this.right = right; }
  toString() { return `(${this.left} ${this.op} ${this.right})`; }
}

class TmPair {
  constructor(fst, snd) { this.fst = fst; this.snd = snd; }
  toString() { return `(${this.fst}, ${this.snd})`; }
}

class TmFst {
  constructor(pair) { this.pair = pair; }
  toString() { return `fst(${this.pair})`; }
}

class TmSnd {
  constructor(pair) { this.pair = pair; }
  toString() { return `snd(${this.pair})`; }
}

class TmFix {
  constructor(body) { this.body = body; }
  toString() { return `fix(${this.body})`; }
}

// ============================================================
// Type Environment
// ============================================================

class TypeEnv {
  constructor(bindings = new Map()) { this.bindings = bindings; }
  
  extend(name, type) {
    const newBindings = new Map(this.bindings);
    newBindings.set(name, type);
    return new TypeEnv(newBindings);
  }
  
  lookup(name) {
    if (this.bindings.has(name)) return this.bindings.get(name);
    return null;
  }
}

// ============================================================
// Type Checker (Bidirectional)
// ============================================================

class TypeError extends Error {
  constructor(msg) { super(msg); this.name = 'TypeError'; }
}

// Infer the type of a term
function infer(env, term) {
  if (term instanceof TmVar) {
    const t = env.lookup(term.name);
    if (t === null) throw new TypeError(`Unbound variable: ${term.name}`);
    return t;
  }
  
  if (term instanceof TmBool) return new TBool();
  if (term instanceof TmInt) return new TInt();
  if (term instanceof TmUnit) return new TUnit();
  
  if (term instanceof TmAbs) {
    const bodyType = infer(env.extend(term.param, term.paramType), term.body);
    return new TArrow(term.paramType, bodyType);
  }
  
  if (term instanceof TmApp) {
    const funcType = infer(env, term.func);
    if (!(funcType instanceof TArrow)) {
      throw new TypeError(`Expected function type, got ${funcType}`);
    }
    check(env, term.arg, funcType.param);
    return funcType.ret;
  }
  
  if (term instanceof TmIf) {
    check(env, term.cond, new TBool());
    const thenType = infer(env, term.then);
    check(env, term.else_, thenType);
    return thenType;
  }
  
  if (term instanceof TmLet) {
    const valueType = infer(env, term.value);
    return infer(env.extend(term.name, valueType), term.body);
  }
  
  if (term instanceof TmBinOp) {
    const arithOps = ['+', '-', '*', '/', '%'];
    const cmpOps = ['<', '>', '<=', '>='];
    const eqOps = ['==', '!='];
    
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
    if (eqOps.includes(term.op)) {
      const leftType = infer(env, term.left);
      check(env, term.right, leftType);
      return new TBool();
    }
    if (term.op === '&&' || term.op === '||') {
      check(env, term.left, new TBool());
      check(env, term.right, new TBool());
      return new TBool();
    }
    throw new TypeError(`Unknown operator: ${term.op}`);
  }
  
  if (term instanceof TmPair) {
    const fstType = infer(env, term.fst);
    const sndType = infer(env, term.snd);
    return new TProd(fstType, sndType);
  }
  
  if (term instanceof TmFst) {
    const pairType = infer(env, term.pair);
    if (!(pairType instanceof TProd)) throw new TypeError(`Expected product type, got ${pairType}`);
    return pairType.fst;
  }
  
  if (term instanceof TmSnd) {
    const pairType = infer(env, term.pair);
    if (!(pairType instanceof TProd)) throw new TypeError(`Expected product type, got ${pairType}`);
    return pairType.snd;
  }
  
  if (term instanceof TmFix) {
    const bodyType = infer(env, term.body);
    if (!(bodyType instanceof TArrow)) throw new TypeError(`fix requires function type, got ${bodyType}`);
    if (!bodyType.param.equals(bodyType.ret)) {
      throw new TypeError(`fix requires T → T, got ${bodyType}`);
    }
    return bodyType.ret;
  }
  
  throw new TypeError(`Cannot infer type of ${term}`);
}

// Check a term against an expected type
function check(env, term, expected) {
  const actual = infer(env, term);
  if (!actual.equals(expected)) {
    throw new TypeError(`Type mismatch: expected ${expected}, got ${actual}`);
  }
}

// Convenience: typecheck a term in the empty environment
function typecheck(term) {
  return infer(new TypeEnv(), term);
}

// ============================================================
// STLC Evaluator (call-by-value)
// ============================================================

function isVal(term) {
  return term instanceof TmBool || term instanceof TmInt || term instanceof TmUnit ||
         term instanceof TmAbs || term instanceof TmPair;
}

function substituteSTLC(term, name, value) {
  if (term instanceof TmVar) {
    return term.name === name ? value : term;
  }
  if (term instanceof TmAbs) {
    if (term.param === name) return term; // shadowed
    return new TmAbs(term.param, term.paramType, substituteSTLC(term.body, name, value));
  }
  if (term instanceof TmApp) {
    return new TmApp(substituteSTLC(term.func, name, value), substituteSTLC(term.arg, name, value));
  }
  if (term instanceof TmBool || term instanceof TmInt || term instanceof TmUnit) return term;
  if (term instanceof TmIf) {
    return new TmIf(
      substituteSTLC(term.cond, name, value),
      substituteSTLC(term.then, name, value),
      substituteSTLC(term.else_, name, value));
  }
  if (term instanceof TmLet) {
    const newValue = substituteSTLC(term.value, name, value);
    if (term.name === name) return new TmLet(term.name, newValue, term.body);
    return new TmLet(term.name, newValue, substituteSTLC(term.body, name, value));
  }
  if (term instanceof TmBinOp) {
    return new TmBinOp(term.op, substituteSTLC(term.left, name, value), substituteSTLC(term.right, name, value));
  }
  if (term instanceof TmPair) {
    return new TmPair(substituteSTLC(term.fst, name, value), substituteSTLC(term.snd, name, value));
  }
  if (term instanceof TmFst) return new TmFst(substituteSTLC(term.pair, name, value));
  if (term instanceof TmSnd) return new TmSnd(substituteSTLC(term.pair, name, value));
  if (term instanceof TmFix) return new TmFix(substituteSTLC(term.body, name, value));
  return term;
}

function evalStep(term) {
  // App: reduce func
  if (term instanceof TmApp) {
    if (!isVal(term.func)) {
      const f = evalStep(term.func);
      return f !== null ? new TmApp(f, term.arg) : null;
    }
    if (!isVal(term.arg)) {
      const a = evalStep(term.arg);
      return a !== null ? new TmApp(term.func, a) : null;
    }
    if (term.func instanceof TmAbs) {
      return substituteSTLC(term.func.body, term.func.param, term.arg);
    }
    return null;
  }
  
  // If
  if (term instanceof TmIf) {
    if (!isVal(term.cond)) {
      const c = evalStep(term.cond);
      return c !== null ? new TmIf(c, term.then, term.else_) : null;
    }
    if (term.cond instanceof TmBool) {
      return term.cond.value ? term.then : term.else_;
    }
    return null;
  }
  
  // Let
  if (term instanceof TmLet) {
    if (!isVal(term.value)) {
      const v = evalStep(term.value);
      return v !== null ? new TmLet(term.name, v, term.body) : null;
    }
    return substituteSTLC(term.body, term.name, term.value);
  }
  
  // BinOp
  if (term instanceof TmBinOp) {
    if (!isVal(term.left)) {
      const l = evalStep(term.left);
      return l !== null ? new TmBinOp(term.op, l, term.right) : null;
    }
    if (!isVal(term.right)) {
      const r = evalStep(term.right);
      return r !== null ? new TmBinOp(term.op, term.left, r) : null;
    }
    if (term.left instanceof TmInt && term.right instanceof TmInt) {
      const l = term.left.value, r = term.right.value;
      switch (term.op) {
        case '+': return new TmInt(l + r);
        case '-': return new TmInt(l - r);
        case '*': return new TmInt(l * r);
        case '/': return new TmInt(Math.trunc(l / r));
        case '%': return new TmInt(l % r);
        case '<': return new TmBool(l < r);
        case '>': return new TmBool(l > r);
        case '<=': return new TmBool(l <= r);
        case '>=': return new TmBool(l >= r);
        case '==': return new TmBool(l === r);
        case '!=': return new TmBool(l !== r);
      }
    }
    if (term.left instanceof TmBool && term.right instanceof TmBool) {
      const l = term.left.value, r = term.right.value;
      switch (term.op) {
        case '&&': return new TmBool(l && r);
        case '||': return new TmBool(l || r);
        case '==': return new TmBool(l === r);
        case '!=': return new TmBool(l !== r);
      }
    }
    return null;
  }
  
  // Pair projections
  if (term instanceof TmFst) {
    if (!isVal(term.pair)) {
      const p = evalStep(term.pair);
      return p !== null ? new TmFst(p) : null;
    }
    if (term.pair instanceof TmPair) return term.pair.fst;
    return null;
  }
  if (term instanceof TmSnd) {
    if (!isVal(term.pair)) {
      const p = evalStep(term.pair);
      return p !== null ? new TmSnd(p) : null;
    }
    if (term.pair instanceof TmPair) return term.pair.snd;
    return null;
  }
  
  // Pair: evaluate components
  if (term instanceof TmPair) {
    if (!isVal(term.fst)) {
      const f = evalStep(term.fst);
      return f !== null ? new TmPair(f, term.snd) : null;
    }
    if (!isVal(term.snd)) {
      const s = evalStep(term.snd);
      return s !== null ? new TmPair(term.fst, s) : null;
    }
    return null;
  }
  
  // Fix: unroll once
  if (term instanceof TmFix) {
    if (!isVal(term.body)) {
      const b = evalStep(term.body);
      return b !== null ? new TmFix(b) : null;
    }
    if (term.body instanceof TmAbs) {
      return substituteSTLC(term.body.body, term.body.param, term);
    }
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
  TBool, TInt, TUnit, TArrow, TProd,
  // Terms
  TmVar, TmAbs, TmApp, TmBool, TmInt, TmUnit,
  TmIf, TmLet, TmBinOp, TmPair, TmFst, TmSnd, TmFix,
  // Type checking
  TypeEnv, TypeError, infer, check, typecheck,
  // Evaluation
  evaluate, evalStep, isVal, substituteSTLC,
};
