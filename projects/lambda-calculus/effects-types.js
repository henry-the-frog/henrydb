/**
 * Type System for Algebraic Effects
 * 
 * Extends the effects system with types:
 * - Effect signatures define operation types
 * - Computation types track which effects may be performed
 * - Handler types show how effects are eliminated
 * - Effect rows for polymorphic effect tracking
 * 
 * Based on:
 * - Koka-style effect rows (Leijen 2014)
 * - Frank-style effect polymorphism (Lindley et al. 2017)
 */

// ============================================================
// Types
// ============================================================

class TNum { toString() { return 'Num'; } tag = 'TNum'; }
class TBool { toString() { return 'Bool'; } tag = 'TBool'; }
class TStr { toString() { return 'Str'; } tag = 'TStr'; }
class TUnit { toString() { return 'Unit'; } tag = 'TUnit'; }

class TVar {
  constructor(name) { this.tag = 'TVar'; this.name = name; }
  toString() { return this.name; }
}

class TFun {
  constructor(param, ret, effects = new EffRow([])) {
    this.tag = 'TFun';
    this.param = param;
    this.ret = ret;
    this.effects = effects;
  }
  toString() {
    const eff = this.effects.isEmpty() ? '' : ` ! ${this.effects}`;
    return `(${this.param} → ${this.ret}${eff})`;
  }
}

class TPair {
  constructor(fst, snd) { this.tag = 'TPair'; this.fst = fst; this.snd = snd; }
  toString() { return `(${this.fst} × ${this.snd})`; }
}

class TList {
  constructor(elem) { this.tag = 'TList'; this.elem = elem; }
  toString() { return `[${this.elem}]`; }
}

// ============================================================
// Effect Types
// ============================================================

// An effect signature: defines the operations and their types
class EffSig {
  constructor(name, operations) {
    this.name = name;
    // operations: Map<opName, {argType, retType}>
    this.operations = operations;
  }
  toString() { return this.name; }
}

// Effect row: a set of effects (with optional row variable for polymorphism)
class EffRow {
  constructor(effects = [], rowVar = null) {
    this.effects = effects; // Array of EffSig names
    this.rowVar = rowVar;   // null or string (for polymorphic rows)
  }
  
  isEmpty() { return this.effects.length === 0 && !this.rowVar; }
  
  has(name) { return this.effects.includes(name); }
  
  without(name) {
    return new EffRow(this.effects.filter(e => e !== name), this.rowVar);
  }
  
  union(other) {
    const combined = [...new Set([...this.effects, ...other.effects])];
    const rv = this.rowVar || other.rowVar;
    return new EffRow(combined, rv);
  }
  
  toString() {
    const parts = [...this.effects];
    if (this.rowVar) parts.push(this.rowVar);
    return `{${parts.join(', ')}}`;
  }
}

// Computation type: value type + effect row
class Comp {
  constructor(valueType, effects = new EffRow([])) {
    this.tag = 'Comp';
    this.valueType = valueType;
    this.effects = effects;
  }
  toString() {
    if (this.effects.isEmpty()) return this.valueType.toString();
    return `${this.valueType} ! ${this.effects}`;
  }
}

// ============================================================
// Built-in Effect Signatures
// ============================================================

const STATE_SIG = new EffSig('State', new Map([
  ['Get', { argType: new TUnit(), retType: new TVar('S') }],
  ['Put', { argType: new TVar('S'), retType: new TUnit() }],
]));

const EXCEPTION_SIG = new EffSig('Exn', new Map([
  ['Raise', { argType: new TVar('E'), retType: new TVar('_bottom') }],
]));

const NONDET_SIG = new EffSig('Nondet', new Map([
  ['Choose', { argType: new TUnit(), retType: new TBool() }],
]));

const LOG_SIG = new EffSig('Log', new Map([
  ['Log', { argType: new TStr(), retType: new TUnit() }],
]));

const EFFECT_REGISTRY = new Map([
  ['State', STATE_SIG],
  ['Exn', EXCEPTION_SIG],
  ['Nondet', NONDET_SIG],
  ['Log', LOG_SIG],
]);

// ============================================================
// Type Environment
// ============================================================

class TypeEnv {
  constructor(parent = null) {
    this.bindings = new Map();
    this.parent = parent;
  }
  
  extend(name, type) {
    const e = new TypeEnv(this);
    e.bindings.set(name, type);
    return e;
  }
  
  lookup(name) {
    if (this.bindings.has(name)) return this.bindings.get(name);
    if (this.parent) return this.parent.lookup(name);
    return null;
  }
}

// ============================================================
// Effect Type Checker
// ============================================================

class EffectTypeError extends Error {
  constructor(msg) { super(msg); this.name = 'EffectTypeError'; }
}

let freshCounter = 0;
function freshVar() { return new TVar(`_e${freshCounter++}`); }
function resetFresh() { freshCounter = 0; }

function inferType(expr, env) {
  if (!expr) return new Comp(new TUnit());
  
  switch (expr.tag) {
    case 'Lit': return new Comp(inferLitType(expr.value));
    
    case 'Var': {
      const type = env.lookup(expr.name);
      if (!type) throw new EffectTypeError(`Unbound variable: ${expr.name}`);
      return new Comp(type);
    }
    
    case 'Lam': {
      const paramType = freshVar();
      const newEnv = env.extend(expr.param, paramType);
      const bodyComp = inferType(expr.body, newEnv);
      return new Comp(new TFun(paramType, bodyComp.valueType, bodyComp.effects));
    }
    
    case 'App': {
      const fnComp = inferType(expr.fn, env);
      const argComp = inferType(expr.arg, env);
      
      if (fnComp.valueType.tag === 'TFun') {
        const retType = fnComp.valueType.ret;
        const effects = fnComp.effects.union(argComp.effects).union(fnComp.valueType.effects);
        return new Comp(retType, effects);
      }
      
      const retType = freshVar();
      return new Comp(retType, fnComp.effects.union(argComp.effects));
    }
    
    case 'Let': {
      const valComp = inferType(expr.value, env);
      const newEnv = env.extend(expr.name, valComp.valueType);
      const bodyComp = inferType(expr.body, newEnv);
      return new Comp(bodyComp.valueType, valComp.effects.union(bodyComp.effects));
    }
    
    case 'If': {
      const condComp = inferType(expr.cond, env);
      const thenComp = inferType(expr.then, env);
      const elseComp = inferType(expr.else_, env);
      const effects = condComp.effects.union(thenComp.effects).union(elseComp.effects);
      return new Comp(thenComp.valueType, effects);
    }
    
    case 'BinOp': {
      const leftComp = inferType(expr.left, env);
      const rightComp = inferType(expr.right, env);
      const resType = inferBinOpType(expr.op, leftComp.valueType, rightComp.valueType);
      return new Comp(resType, leftComp.effects.union(rightComp.effects));
    }
    
    case 'MkPair': {
      const fstComp = inferType(expr.fst, env);
      const sndComp = inferType(expr.snd, env);
      return new Comp(
        new TPair(fstComp.valueType, sndComp.valueType),
        fstComp.effects.union(sndComp.effects));
    }
    
    case 'Fst': {
      const pairComp = inferType(expr.pair, env);
      if (pairComp.valueType.tag === 'TPair') {
        return new Comp(pairComp.valueType.fst, pairComp.effects);
      }
      return new Comp(freshVar(), pairComp.effects);
    }
    
    case 'Snd': {
      const pairComp = inferType(expr.pair, env);
      if (pairComp.valueType.tag === 'TPair') {
        return new Comp(pairComp.valueType.snd, pairComp.effects);
      }
      return new Comp(freshVar(), pairComp.effects);
    }
    
    case 'Perform': {
      const sig = EFFECT_REGISTRY.get(findEffectForOp(expr.effect));
      if (!sig) {
        // Unknown effect — add generic effect
        const retType = freshVar();
        return new Comp(retType, new EffRow([expr.effect]));
      }
      const op = sig.operations.get(expr.effect);
      return new Comp(op.retType, new EffRow([sig.name]));
    }
    
    case 'Handle': {
      const bodyComp = inferType(expr.body, env);
      
      // Determine which effects are handled
      const handledEffects = Object.keys(expr.handler.ops || {}).map(findEffectForOp).filter(Boolean);
      
      // Remove handled effects from the row
      let remainingEffects = bodyComp.effects;
      for (const eff of handledEffects) {
        remainingEffects = remainingEffects.without(eff);
      }
      
      return new Comp(bodyComp.valueType, remainingEffects);
    }
    
    default:
      return new Comp(freshVar());
  }
}

function findEffectForOp(opName) {
  for (const [name, sig] of EFFECT_REGISTRY) {
    if (sig.operations.has(opName)) return name;
  }
  return opName; // Use op name as effect name if not registered
}

function inferLitType(value) {
  if (!value) return new TUnit();
  switch (value.tag) {
    case 'Num': return new TNum();
    case 'Bool': return new TBool();
    case 'Str': return new TStr();
    case 'Unit': return new TUnit();
    case 'Pair': return new TPair(inferLitType(value.fst), inferLitType(value.snd));
    case 'ListVal': return new TList(value.elems.length > 0 ? inferLitType(value.elems[0]) : freshVar());
    default: return freshVar();
  }
}

function inferBinOpType(op, leftType, rightType) {
  switch (op) {
    case '+': case '-': case '*': case '/': case '%': return new TNum();
    case '==': case '<': case '>': case '<=': case '>=': return new TBool();
    case '++': return new TStr();
    default: return freshVar();
  }
}

// ============================================================
// Public API
// ============================================================

function typeOf(expr, env = new TypeEnv()) {
  resetFresh();
  return inferType(expr, env);
}

export {
  // Types
  TNum, TBool, TStr, TUnit, TVar, TFun, TPair, TList,
  // Effects
  EffSig, EffRow, Comp,
  // Registry
  STATE_SIG, EXCEPTION_SIG, NONDET_SIG, LOG_SIG, EFFECT_REGISTRY,
  // Type environment
  TypeEnv,
  // Type checker
  typeOf, inferType, EffectTypeError, resetFresh,
};
