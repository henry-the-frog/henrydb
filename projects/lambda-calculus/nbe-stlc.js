/**
 * Normalization by Evaluation (NbE) for STLC
 * 
 * Two phases:
 * 1. EVAL: interpret syntax into semantic domain (host language values)
 * 2. READBACK: quote semantic values back to normal forms
 * 
 * The key insight: the host language's evaluation does the β-reductions for us.
 * We get normalization "for free" via the metalanguage.
 * 
 * Handles: variables, application, λ-abstraction, let bindings, constants
 */

// Syntax
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Add { constructor(l, r) { this.tag = 'Add'; this.left = l; this.right = r; } toString() { return `(${this.left} + ${this.right})`; } }
class If0 { constructor(cond, t, f) { this.tag = 'If0'; this.cond = cond; this.then = t; this.else = f; } }

// Semantic domain
class VLam { constructor(fn) { this.tag = 'VLam'; this.fn = fn; } } // JS function
class VNum { constructor(n) { this.tag = 'VNum'; this.n = n; } }
class VNeutral { constructor(neutral) { this.tag = 'VNeutral'; this.neutral = neutral; } } // stuck term

// Neutral terms (can't reduce further)
class NVar { constructor(name) { this.tag = 'NVar'; this.name = name; } }
class NApp { constructor(fn, arg) { this.tag = 'NApp'; this.fn = fn; this.arg = arg; } }
class NAdd { constructor(l, r) { this.tag = 'NAdd'; this.left = l; this.right = r; } }
class NIf0 { constructor(c, t, f) { this.tag = 'NIf0'; this.cond = c; this.then = t; this.else = f; } }

// ============================================================
// Phase 1: Evaluation (syntax → semantic values)
// ============================================================

function evaluate(expr, env = new Map()) {
  switch (expr.tag) {
    case 'Num': return new VNum(expr.n);
    case 'Var': return env.get(expr.name) || new VNeutral(new NVar(expr.name));
    case 'Lam': return new VLam(val => evaluate(expr.body, new Map([...env, [expr.var, val]])));
    case 'App': return doApply(evaluate(expr.fn, env), evaluate(expr.arg, env));
    case 'Let': {
      const val = evaluate(expr.init, env);
      return evaluate(expr.body, new Map([...env, [expr.var, val]]));
    }
    case 'Add': return doAdd(evaluate(expr.left, env), evaluate(expr.right, env));
    case 'If0': return doIf0(evaluate(expr.cond, env), () => evaluate(expr.then, env), () => evaluate(expr.else, env));
    default: throw new Error(`Unknown: ${expr.tag}`);
  }
}

function doApply(fn, arg) {
  if (fn.tag === 'VLam') return fn.fn(arg);
  if (fn.tag === 'VNeutral') return new VNeutral(new NApp(fn.neutral, arg));
  throw new Error('Not a function');
}

function doAdd(left, right) {
  if (left.tag === 'VNum' && right.tag === 'VNum') return new VNum(left.n + right.n);
  return new VNeutral(new NAdd(left, right));
}

function doIf0(cond, thenFn, elseFn) {
  if (cond.tag === 'VNum') return cond.n === 0 ? thenFn() : elseFn();
  return new VNeutral(new NIf0(cond, thenFn(), elseFn()));
}

// ============================================================
// Phase 2: Readback (semantic values → normal forms)
// ============================================================

let freshCounter = 0;
function fresh(base = 'x') { return `${base}${freshCounter++}`; }
function resetFresh() { freshCounter = 0; }

function readback(value) {
  switch (value.tag) {
    case 'VNum': return new Num(value.n);
    case 'VLam': {
      const x = fresh();
      const body = readback(value.fn(new VNeutral(new NVar(x))));
      return new Lam(x, body);
    }
    case 'VNeutral': return readbackNeutral(value.neutral);
    default: throw new Error(`Unknown value: ${value.tag}`);
  }
}

function readbackNeutral(neutral) {
  switch (neutral.tag) {
    case 'NVar': return new Var(neutral.name);
    case 'NApp': return new App(readbackNeutral(neutral.fn), readback(neutral.arg));
    case 'NAdd': return new Add(readback(neutral.left), readback(neutral.right));
    case 'NIf0': return new If0(readbackNeutral(neutral.cond), readback(neutral.then), readback(neutral.else));
    default: throw new Error(`Unknown neutral: ${neutral.tag}`);
  }
}

// ============================================================
// Normalize: eval then readback
// ============================================================

function normalize(expr, env = new Map()) {
  resetFresh();
  return readback(evaluate(expr, env));
}

function normToString(expr, env) {
  return normalize(expr, env).toString();
}

export {
  Var, Lam, App, Let, Num, Add, If0,
  evaluate, readback, normalize, normToString, resetFresh
};
