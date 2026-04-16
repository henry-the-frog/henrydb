/**
 * CEK Machine: An Abstract Machine for Lambda Calculus
 * 
 * The CEK machine makes evaluation explicit:
 * - C (Control): the expression currently being evaluated
 * - E (Environment): variable bindings (closure environment)
 * - K (Kontinuation): what to do with the result (explicit stack)
 * 
 * This bridges theory and implementation:
 * - Lambda calculus reduction rules → machine transitions
 * - No substitution needed (environments replace it)
 * - Continuation = explicit call stack
 * 
 * Based on Felleisen & Friedman (1986).
 */

// ============================================================
// Expressions
// ============================================================

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(param, body) { this.tag = 'Lam'; this.param = param; this.body = body; } toString() { return `(λ${this.param}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Prim { constructor(op, left, right) { this.tag = 'Prim'; this.op = op; this.left = left; this.right = right; } toString() { return `(${this.left} ${this.op} ${this.right})`; } }
class If0 { constructor(cond, then, els) { this.tag = 'If0'; this.cond = cond; this.then = then; this.els = els; } toString() { return `(if0 ${this.cond} ${this.then} ${this.els})`; } }
class Let { constructor(name, val, body) { this.tag = 'Let'; this.name = name; this.val = val; this.body = body; } toString() { return `(let ${this.name} = ${this.val} in ${this.body})`; } }

// ============================================================
// Values
// ============================================================

class NumVal { constructor(n) { this.tag = 'NumVal'; this.n = n; } toString() { return `${this.n}`; } }
class Closure { constructor(param, body, env) { this.tag = 'Closure'; this.param = param; this.body = body; this.env = env; } toString() { return `<closure λ${this.param}>`; } }

// ============================================================
// Environment
// ============================================================

class Env {
  constructor(bindings = new Map()) { this.bindings = bindings; }
  lookup(name) {
    if (this.bindings.has(name)) return this.bindings.get(name);
    throw new Error(`CEK: Unbound variable: ${name}`);
  }
  extend(name, value) {
    const newBindings = new Map(this.bindings);
    newBindings.set(name, value);
    return new Env(newBindings);
  }
}

// ============================================================
// Continuations (explicit stack frames)
// ============================================================

class Halt { constructor() { this.tag = 'Halt'; } toString() { return '□'; } }

// Evaluating function position: after fn evaluates, evaluate arg
class ArgK { constructor(arg, env, k) { this.tag = 'ArgK'; this.arg = arg; this.env = env; this.k = k; } }

// Function evaluated, now evaluating argument
class FnK { constructor(fn, k) { this.tag = 'FnK'; this.fn = fn; this.k = k; } }

// Evaluating left operand of primitive
class PrimLK { constructor(op, right, env, k) { this.tag = 'PrimLK'; this.op = op; this.right = right; this.env = env; this.k = k; } }

// Left operand evaluated, evaluating right
class PrimRK { constructor(op, left, k) { this.tag = 'PrimRK'; this.op = op; this.left = left; this.k = k; } }

// Evaluating condition of if0
class If0K { constructor(then, els, env, k) { this.tag = 'If0K'; this.then = then; this.els = els; this.env = env; this.k = k; } }

// Evaluating let value
class LetK { constructor(name, body, env, k) { this.tag = 'LetK'; this.name = name; this.body = body; this.env = env; this.k = k; } }

// ============================================================
// CEK Machine
// ============================================================

class CEKMachine {
  constructor(options = {}) {
    this.maxSteps = options.maxSteps || 1000;
    this.trace = options.trace || false;
    this.steps = [];
  }

  /**
   * Evaluate an expression to a value
   */
  eval(expr) {
    let c = expr;           // Control
    let e = new Env();      // Environment
    let k = new Halt();     // Continuation
    let stepCount = 0;

    while (stepCount < this.maxSteps) {
      if (this.trace) {
        this.steps.push({ step: stepCount, control: c.toString(), kTag: k.tag });
      }
      stepCount++;

      const transition = this._step(c, e, k);
      if (transition.done) return { value: transition.value, steps: stepCount };
      c = transition.c;
      e = transition.e;
      k = transition.k;
    }

    throw new Error(`CEK: diverged after ${this.maxSteps} steps`);
  }

  _step(c, e, k) {
    switch (c.tag) {
      case 'Num':
        return this._applyCont(k, new NumVal(c.n));
      
      case 'Var':
        return this._applyCont(k, e.lookup(c.name));
      
      case 'Lam':
        return this._applyCont(k, new Closure(c.param, c.body, e));
      
      case 'App':
        // Evaluate function first, push ArgK continuation
        return { c: c.fn, e, k: new ArgK(c.arg, e, k) };
      
      case 'Prim':
        // Evaluate left operand first
        return { c: c.left, e, k: new PrimLK(c.op, c.right, e, k) };
      
      case 'If0':
        // Evaluate condition first
        return { c: c.cond, e, k: new If0K(c.then, c.els, e, k) };
      
      case 'Let':
        // Evaluate value first
        return { c: c.val, e, k: new LetK(c.name, c.body, e, k) };
      
      default:
        throw new Error(`CEK: unknown expression: ${c.tag}`);
    }
  }

  _applyCont(k, val) {
    switch (k.tag) {
      case 'Halt':
        return { done: true, value: val };
      
      case 'ArgK':
        // Function evaluated → now evaluate argument
        return { c: k.arg, e: k.env, k: new FnK(val, k.k) };
      
      case 'FnK':
        // Both fn and arg evaluated → apply
        if (k.fn.tag !== 'Closure') throw new Error(`CEK: not a function: ${k.fn}`);
        return { c: k.fn.body, e: k.fn.env.extend(k.fn.param, val), k: k.k };
      
      case 'PrimLK':
        // Left operand evaluated → evaluate right
        return { c: k.right, e: k.env, k: new PrimRK(k.op, val, k.k) };
      
      case 'PrimRK':
        // Both operands evaluated → compute
        return this._applyCont(k.k, this._evalPrim(k.op, k.left, val));
      
      case 'If0K':
        // Condition evaluated → branch
        if (val.tag === 'NumVal' && val.n === 0) {
          return { c: k.then, e: k.env, k: k.k };
        }
        return { c: k.els, e: k.env, k: k.k };
      
      case 'LetK':
        // Value evaluated → bind and evaluate body
        return { c: k.body, e: k.env.extend(k.name, val), k: k.k };
      
      default:
        throw new Error(`CEK: unknown continuation: ${k.tag}`);
    }
  }

  _evalPrim(op, left, right) {
    if (left.tag !== 'NumVal' || right.tag !== 'NumVal') {
      throw new Error(`CEK: primitive ${op} requires numbers`);
    }
    switch (op) {
      case '+': return new NumVal(left.n + right.n);
      case '-': return new NumVal(left.n - right.n);
      case '*': return new NumVal(left.n * right.n);
      case '/': return new NumVal(Math.floor(left.n / right.n));
      case '%': return new NumVal(left.n % right.n);
      default: throw new Error(`CEK: unknown primitive: ${op}`);
    }
  }
}

// ============================================================
// Convenience constructors
// ============================================================

const v = name => new Var(name);
const lam = (p, b) => new Lam(p, b);
const app = (f, a) => new App(f, a);
const n = x => new Num(x);
const prim = (op, l, r) => new Prim(op, l, r);
const if0 = (c, t, e) => new If0(c, t, e);
const let_ = (name, val, body) => new Let(name, val, body);

// ============================================================
// Exports
// ============================================================

export {
  Var, Lam, App, Num, Prim, If0, Let,
  NumVal, Closure,
  Env, CEKMachine,
  Halt, ArgK, FnK, PrimLK, PrimRK, If0K, LetK,
  v, lam, app, n, prim, if0, let_
};
