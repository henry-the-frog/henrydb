/**
 * Effect Polymorphism
 * 
 * Functions parameterized over their effect set:
 *   map : ∀ε. (a -{ε}→ b) → List a -{ε}→ List b
 * 
 * Meaning: map's effects = whatever effects the function argument has.
 * If passed a pure function → pure. If passed IO function → IO.
 * 
 * This enables writing effect-generic code.
 */

// Effects
class EEmpty { constructor() { this.tag = 'EEmpty'; } toString() { return '{}'; } }
class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } toString() { return this.name; } }
class ESet {
  constructor(effects) { this.tag = 'ESet'; this.effects = effects; }
  toString() { return `{${this.effects.join(', ')}}`; }
}
class EUnion {
  constructor(left, right) { this.tag = 'EUnion'; this.left = left; this.right = right; }
  toString() { return `${this.left} ∪ ${this.right}`; }
}

const ePure = new EEmpty();
const eIO = new ESet(['IO']);
const eExc = new ESet(['Exception']);
const eState = new ESet(['State']);

// Types with effect annotations
class TFun {
  constructor(param, effect, ret) {
    this.tag = 'TFun';
    this.param = param;
    this.effect = effect;
    this.ret = ret;
  }
  toString() { return `(${this.param} -{${this.effect}}→ ${this.ret})`; }
}

class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TList { constructor(elem) { this.tag = 'TList'; this.elem = elem; } toString() { return `[${this.elem}]`; } }

const tInt = new TBase('Int');
const tStr = new TBase('Str');
const tBool = new TBase('Bool');

// ============================================================
// Effect operations
// ============================================================

function effectUnion(e1, e2) {
  if (e1.tag === 'EEmpty') return e2;
  if (e2.tag === 'EEmpty') return e1;
  if (effectEquals(e1, e2)) return e1;
  
  const s1 = effectToSet(e1);
  const s2 = effectToSet(e2);
  const combined = new Set([...s1, ...s2]);
  if (combined.size === 0) return ePure;
  return new ESet([...combined]);
}

function effectToSet(effect) {
  if (effect.tag === 'EEmpty') return new Set();
  if (effect.tag === 'ESet') return new Set(effect.effects);
  if (effect.tag === 'EUnion') return new Set([...effectToSet(effect.left), ...effectToSet(effect.right)]);
  return new Set(); // EVar treated as unknown
}

function effectEquals(a, b) {
  if (a.tag === 'EEmpty' && b.tag === 'EEmpty') return true;
  if (a.tag === 'ESet' && b.tag === 'ESet') {
    if (a.effects.length !== b.effects.length) return false;
    return a.effects.every(e => b.effects.includes(e));
  }
  if (a.tag === 'EVar' && b.tag === 'EVar') return a.name === b.name;
  return false;
}

function effectSubset(a, b) {
  const sa = effectToSet(a);
  const sb = effectToSet(b);
  return [...sa].every(e => sb.has(e));
}

function isPure(effect) {
  return effect.tag === 'EEmpty' || (effect.tag === 'ESet' && effect.effects.length === 0);
}

// ============================================================
// Effect inference
// ============================================================

function inferEffect(expr, env = new Map()) {
  switch (expr.tag) {
    case 'ENum': case 'EBool': case 'EStr': return ePure;
    case 'EVar': return env.get(expr.name)?.effect || ePure;
    case 'EApp': {
      const fnEffect = inferEffect(expr.fn, env);
      const argEffect = inferEffect(expr.arg, env);
      const callEffect = env.get(expr.fn?.name)?.callEffect || ePure;
      return effectUnion(fnEffect, effectUnion(argEffect, callEffect));
    }
    case 'EPerform': return expr.effect;
    case 'ESeq': {
      const e1 = inferEffect(expr.first, env);
      const e2 = inferEffect(expr.second, env);
      return effectUnion(e1, e2);
    }
    default: return ePure;
  }
}

// Simple expression types for inference
class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } }
class EBool { constructor(v) { this.tag = 'EBool'; this.v = v; } }
class EStr { constructor(s) { this.tag = 'EStr'; this.s = s; } }
class EVar2 { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class EApp2 { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class EPerform { constructor(effect) { this.tag = 'EPerform'; this.effect = effect; } }
class ESeq { constructor(first, second) { this.tag = 'ESeq'; this.first = first; this.second = second; } }

export {
  EEmpty, EVar, ESet, EUnion,
  ePure, eIO, eExc, eState,
  TFun, TBase, TList, tInt, tStr, tBool,
  effectUnion, effectEquals, effectSubset, isPure, effectToSet,
  inferEffect,
  ENum, EBool, EStr, EVar2, EApp2, EPerform, ESeq
};
