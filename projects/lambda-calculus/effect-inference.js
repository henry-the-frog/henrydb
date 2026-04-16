/**
 * Effect Inference: Infer which effects a function uses from its code
 * 
 * Given unannotated code, determine its effect type automatically.
 * Like type inference, but for effects.
 */

class EffVar { constructor(name) { this.tag = 'EffVar'; this.name = name; } }
class EffEmpty { constructor() { this.tag = 'EffEmpty'; } }
class EffSingle { constructor(name) { this.tag = 'EffSingle'; this.name = name; } }
class EffUnion { constructor(effs) { this.tag = 'EffUnion'; this.effects = effs; } }

// Expressions
class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } }
class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class ELam { constructor(v, body) { this.tag = 'ELam'; this.var = v; this.body = body; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class EPerform { constructor(effect, op) { this.tag = 'EPerform'; this.effect = effect; this.op = op; } }
class EHandle { constructor(body, effect, handler) { this.tag = 'EHandle'; this.body = body; this.effect = effect; this.handler = handler; } }
class ESeq { constructor(first, second) { this.tag = 'ESeq'; this.first = first; this.second = second; } }
class ELet { constructor(v, init, body) { this.tag = 'ELet'; this.var = v; this.init = init; this.body = body; } }

class EffectInferrer {
  constructor() { this.constraints = []; this.nextVar = 0; }
  
  freshEffVar() { return new EffVar(`ε${this.nextVar++}`); }
  
  infer(expr, env = new Map()) {
    switch (expr.tag) {
      case 'ENum': return new EffEmpty();
      case 'EVar': return env.get(expr.name)?.effect || new EffEmpty();
      case 'EPerform': return new EffSingle(expr.effect);
      case 'ESeq': return this.union(this.infer(expr.first, env), this.infer(expr.second, env));
      case 'ELam': {
        const bodyEff = this.infer(expr.body, new Map([...env, [expr.var, { effect: new EffEmpty() }]]));
        return new EffEmpty(); // Lambda itself is pure; its body's effect is latent
      }
      case 'EApp': return this.union(this.infer(expr.fn, env), this.infer(expr.arg, env));
      case 'ELet': {
        const initEff = this.infer(expr.init, env);
        const bodyEff = this.infer(expr.body, new Map([...env, [expr.var, { effect: initEff }]]));
        return this.union(initEff, bodyEff);
      }
      case 'EHandle': {
        const bodyEff = this.infer(expr.body, env);
        return this.subtract(bodyEff, expr.effect);
      }
      default: return new EffEmpty();
    }
  }
  
  union(e1, e2) {
    if (e1.tag === 'EffEmpty') return e2;
    if (e2.tag === 'EffEmpty') return e1;
    const set = new Set([...this.toSet(e1), ...this.toSet(e2)]);
    if (set.size === 0) return new EffEmpty();
    if (set.size === 1) return new EffSingle([...set][0]);
    return new EffUnion([...set]);
  }
  
  subtract(eff, effectName) {
    const set = this.toSet(eff);
    set.delete(effectName);
    if (set.size === 0) return new EffEmpty();
    if (set.size === 1) return new EffSingle([...set][0]);
    return new EffUnion([...set]);
  }
  
  toSet(eff) {
    if (eff.tag === 'EffEmpty') return new Set();
    if (eff.tag === 'EffSingle') return new Set([eff.name]);
    if (eff.tag === 'EffUnion') return new Set(eff.effects);
    return new Set();
  }
  
  effectString(eff) {
    if (eff.tag === 'EffEmpty') return '{}';
    if (eff.tag === 'EffSingle') return `{${eff.name}}`;
    if (eff.tag === 'EffUnion') return `{${eff.effects.join(', ')}}`;
    return '?';
  }
}

export { EffVar, EffEmpty, EffSingle, EffUnion, ENum, EVar, ELam, EApp, EPerform, EHandle, ESeq, ELet, EffectInferrer };
