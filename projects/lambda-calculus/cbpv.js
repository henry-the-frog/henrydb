/**
 * Call-by-Push-Value (CBPV) — Paul Levy
 * 
 * Unifies call-by-name and call-by-value in a single calculus.
 * 
 * Two kinds of types:
 * - Value types (A): data that just sits there (Int, Bool, products)
 * - Computation types (B): things that DO stuff (functions, effects)
 * 
 * Key connectives:
 * - F(A): "thunk" a computation that produces A (value → computation)
 * - U(B): "force" extract a value from a thunked computation (computation → value)
 * - return: value → computation
 * - to: bind (sequence computations)
 */

// Value types
class VInt { constructor() { this.tag = 'VInt'; } toString() { return 'Int'; } }
class VBool { constructor() { this.tag = 'VBool'; } toString() { return 'Bool'; } }
class VUnit { constructor() { this.tag = 'VUnit'; } toString() { return '1'; } }
class VProd { constructor(l, r) { this.tag = 'VProd'; this.left = l; this.right = r; } toString() { return `(${this.left} × ${this.right})`; } }
class VU { constructor(comp) { this.tag = 'VU'; this.comp = comp; } toString() { return `U(${this.comp})`; } } // Thunked computation

// Computation types
class CRet { constructor(val) { this.tag = 'CRet'; this.val = val; } toString() { return `F(${this.val})`; } } // Producer
class CArrow { constructor(param, body) { this.tag = 'CArrow'; this.param = param; this.body = body; } toString() { return `${this.param} → ${this.body}`; } }
class CProd { constructor(l, r) { this.tag = 'CProd'; this.left = l; this.right = r; } toString() { return `${this.left} & ${this.right}`; } }

// Terms
class EVal { constructor(n) { this.tag = 'EVal'; this.n = n; } }
class EBool { constructor(v) { this.tag = 'EBool'; this.v = v; } }
class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class EThunk { constructor(comp) { this.tag = 'EThunk'; this.comp = comp; } } // Value: freeze a computation
class EForce { constructor(val) { this.tag = 'EForce'; this.val = val; } }   // Computation: run a thunk
class ERet { constructor(val) { this.tag = 'ERet'; this.val = val; } }       // return: value → computation
class ETo { constructor(comp, v, body) { this.tag = 'ETo'; this.comp = comp; this.var = v; this.body = body; } } // bind
class ELam { constructor(v, body) { this.tag = 'ELam'; this.var = v; this.body = body; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }

// ============================================================
// CBPV Evaluator
// ============================================================

class CBPVMachine {
  constructor() { this.steps = 0; }

  evalValue(expr, env = new Map()) {
    switch (expr.tag) {
      case 'EVal': return { tag: 'VNum', n: expr.n };
      case 'EBool': return { tag: 'VBool', v: expr.v };
      case 'EVar': {
        const v = env.get(expr.name);
        if (v === undefined) throw new Error(`Unbound: ${expr.name}`);
        return v;
      }
      case 'EThunk': return { tag: 'VThunk', comp: expr.comp, env: new Map(env) };
      default: throw new Error(`Not a value: ${expr.tag}`);
    }
  }

  evalComp(expr, env = new Map()) {
    this.steps++;
    if (this.steps > 10000) throw new Error('Step limit');
    
    switch (expr.tag) {
      case 'ERet': return { tag: 'CVal', value: this.evalValue(expr.val, env) };
      case 'ETo': {
        const result = this.evalComp(expr.comp, env);
        if (result.tag !== 'CVal') throw new Error('Expected value from computation');
        return this.evalComp(expr.body, new Map([...env, [expr.var, result.value]]));
      }
      case 'EForce': {
        const thunk = this.evalValue(expr.val, env);
        if (thunk.tag !== 'VThunk') throw new Error('Force: not a thunk');
        return this.evalComp(thunk.comp, thunk.env);
      }
      case 'ELam': return { tag: 'CClosure', var: expr.var, body: expr.body, env: new Map(env) };
      case 'EApp': {
        const fn = this.evalComp(expr.fn, env);
        if (fn.tag !== 'CClosure') throw new Error('Apply: not a function');
        const arg = this.evalValue(expr.arg, env);
        return this.evalComp(fn.body, new Map([...fn.env, [fn.var, arg]]));
      }
      default:
        return this.evalValue(expr, env);
    }
  }

  run(expr) {
    this.steps = 0;
    return this.evalComp(expr);
  }
}

export {
  VInt, VBool, VUnit, VProd, VU, CRet, CArrow, CProd,
  EVal, EBool, EVar, EThunk, EForce, ERet, ETo, ELam, EApp,
  CBPVMachine
};
