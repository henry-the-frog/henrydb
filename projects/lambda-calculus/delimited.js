/**
 * Delimited Continuations: shift/reset
 * 
 * Delimited continuations capture "the rest of the computation up to a delimiter".
 * They subsume: exceptions, generators, coroutines, and algebraic effects.
 * 
 *   reset(() => 1 + shift(k => k(k(2))))
 *   = 1 + (1 + 2) = 4
 * 
 * Based on:
 * - Danvy & Filinski (1990) shift/reset
 * - Felleisen (1988) control/prompt
 */

// ============================================================
// Values
// ============================================================

class Num {
  constructor(n) { this.tag = 'Num'; this.n = n; }
  toString() { return `${this.n}`; }
}

class Bool {
  constructor(v) { this.tag = 'Bool'; this.v = v; }
  toString() { return `${this.v}`; }
}

class Str {
  constructor(s) { this.tag = 'Str'; this.s = s; }
  toString() { return `"${this.s}"`; }
}

class Fn {
  constructor(param, body, env) {
    this.tag = 'Fn'; this.param = param; this.body = body; this.env = env;
  }
  toString() { return `<fn ${this.param}>`; }
}

class Cont {
  constructor(fn) { this.tag = 'Cont'; this.fn = fn; } // JS function
  toString() { return '<continuation>'; }
}

class ListVal {
  constructor(elems) { this.tag = 'ListVal'; this.elems = elems; }
  toString() { return `[${this.elems.join(', ')}]`; }
}

// ============================================================
// Expressions
// ============================================================

class ELit { constructor(val) { this.tag = 'ELit'; this.val = val; } }
class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class ELam { constructor(param, body) { this.tag = 'ELam'; this.param = param; this.body = body; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class ELet { constructor(name, val, body) { this.tag = 'ELet'; this.name = name; this.val = val; this.body = body; } }
class EIf { constructor(cond, t, f) { this.tag = 'EIf'; this.cond = cond; this.t = t; this.f = f; } }
class EOp { constructor(op, l, r) { this.tag = 'EOp'; this.op = op; this.l = l; this.r = r; } }

// Delimited continuation operators
class EReset { constructor(body) { this.tag = 'EReset'; this.body = body; } }
class EShift { constructor(kParam, body) { this.tag = 'EShift'; this.kParam = kParam; this.body = body; } }

// ============================================================
// Environment
// ============================================================

class Env {
  constructor(bindings = new Map(), parent = null) {
    this.bindings = bindings; this.parent = parent;
  }
  lookup(name) {
    if (this.bindings.has(name)) return this.bindings.get(name);
    if (this.parent) return this.parent.lookup(name);
    throw new Error(`Unbound: ${name}`);
  }
  extend(name, val) {
    const newBindings = new Map(this.bindings);
    newBindings.set(name, val);
    return new Env(newBindings, this.parent);
  }
}

// ============================================================
// CPS Evaluator with shift/reset
// ============================================================

// Signal for shift
class ShiftSignal {
  constructor(kParam, body, env) {
    this.kParam = kParam;
    this.body = body;
    this.env = env;
  }
}

/**
 * Evaluate with delimited continuations.
 * Uses direct-style evaluation with exceptions for shift.
 */
function evaluate(expr, env = new Env()) {
  return evalCPS(expr, env, x => x);
}

function evalCPS(expr, env, k) {
  switch (expr.tag) {
    case 'ELit': return k(expr.val);
    
    case 'EVar': return k(env.lookup(expr.name));
    
    case 'ELam': return k(new Fn(expr.param, expr.body, env));
    
    case 'EApp':
      return evalCPS(expr.fn, env, fn =>
        evalCPS(expr.arg, env, arg =>
          applyCPS(fn, arg, k)));
    
    case 'ELet':
      return evalCPS(expr.val, env, val =>
        evalCPS(expr.body, env.extend(expr.name, val), k));
    
    case 'EIf':
      return evalCPS(expr.cond, env, cond =>
        cond.v
          ? evalCPS(expr.t, env, k)
          : evalCPS(expr.f, env, k));
    
    case 'EOp':
      return evalCPS(expr.l, env, l =>
        evalCPS(expr.r, env, r =>
          k(evalOp(expr.op, l, r))));
    
    case 'EReset':
      // Reset delimits the continuation
      // Evaluate body with identity continuation, then pass to outer k
      return k(evalCPS(expr.body, env, x => x));
    
    case 'EShift':
      // Capture the continuation k and make it available as a first-class value
      // The captured k includes everything up to the nearest reset
      const capturedK = new Cont(v => k(v));
      // Evaluate the shift body with k bound, using identity as the new continuation
      return evalCPS(expr.body, env.extend(expr.kParam, capturedK), x => x);
    
    default:
      throw new Error(`Unknown expression: ${expr.tag}`);
  }
}

function applyCPS(fn, arg, k) {
  if (fn.tag === 'Fn') {
    return evalCPS(fn.body, fn.env.extend(fn.param, arg), k);
  }
  if (fn.tag === 'Cont') {
    // Applying a captured continuation
    return fn.fn(arg);
  }
  throw new Error(`Cannot apply: ${fn.tag}`);
}

function evalOp(op, l, r) {
  switch (op) {
    case '+': return new Num(l.n + r.n);
    case '-': return new Num(l.n - r.n);
    case '*': return new Num(l.n * r.n);
    case '/': return new Num(Math.floor(l.n / r.n));
    case '%': return new Num(l.n % r.n);
    case '==': return new Bool(l.n === r.n);
    case '<': return new Bool(l.n < r.n);
    case '>': return new Bool(l.n > r.n);
    case '++': return new Str(l.s + r.s);
    default: throw new Error(`Unknown op: ${op}`);
  }
}

// ============================================================
// Convenience constructors
// ============================================================

function lit(v) { return new ELit(v); }
function num(n) { return new ELit(new Num(n)); }
function bool(b) { return new ELit(new Bool(b)); }
function str(s) { return new ELit(new Str(s)); }
function evar(name) { return new EVar(name); }
function lam(param, body) { return new ELam(param, body); }
function app(fn, ...args) { return args.reduce((f, a) => new EApp(f, a), fn); }
function elet(name, val, body) { return new ELet(name, val, body); }
function eif(cond, t, f) { return new EIf(cond, t, f); }
function op(o, l, r) { return new EOp(o, l, r); }
function reset(body) { return new EReset(body); }
function shift(kParam, body) { return new EShift(kParam, body); }

// ============================================================
// Example Programs
// ============================================================

// Abort (like exceptions): shift that ignores continuation
function abort(value) {
  return shift('_k', value);
}

// Yield (like generators): shift that uses continuation once
function yieldVal(value) {
  return shift('k', app(evar('k'), value));
}

// ============================================================
// Exports
// ============================================================

export {
  Num, Bool, Str, Fn, Cont, ListVal,
  ELit, EVar, ELam, EApp, ELet, EIf, EOp, EReset, EShift,
  evaluate, evalCPS, Env,
  lit, num, bool, str, evar, lam, app, elet, eif, op, reset, shift,
  abort, yieldVal
};
