/**
 * Combinatory Logic: S, K, I, B, C combinators
 * 
 * No variables! Everything is expressed with combinators:
 * I = λx.x               (identity)
 * K = λx.λy.x            (constant)
 * S = λf.λg.λx.f x (g x) (application)
 * B = λf.λg.λx.f (g x)   (composition)
 * C = λf.λx.λy.f y x      (flip)
 * 
 * S and K are sufficient (SKI basis). I = S K K.
 */

// Combinators as JS functions
const I = x => x;
const K = x => y => x;
const S = f => g => x => f(x)(g(x));
const B = f => g => x => f(g(x));       // B = S (K S) K
const C = f => x => y => f(y)(x);       // C = S (S (K (S (K S) K)) S) (K K)
const W = f => x => f(x)(x);             // W = S S (S K)
const T = x => f => f(x);                // Thrush (flip of apply)

// AST for combinator terms
class CombI { constructor() { this.tag = 'I'; } toString() { return 'I'; } }
class CombK { constructor() { this.tag = 'K'; } toString() { return 'K'; } }
class CombS { constructor() { this.tag = 'S'; } toString() { return 'S'; } }
class CombB { constructor() { this.tag = 'B'; } toString() { return 'B'; } }
class CombC { constructor() { this.tag = 'C'; } toString() { return 'C'; } }
class CombApp { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class CombVar { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }

// Reduction
function combStep(expr) {
  // I x → x
  if (expr.tag === 'App' && expr.fn.tag === 'I') return expr.arg;
  // K x y → x
  if (expr.tag === 'App' && expr.fn.tag === 'App' && expr.fn.fn.tag === 'K') return expr.fn.arg;
  // S f g x → f x (g x)
  if (expr.tag === 'App' && expr.fn.tag === 'App' && expr.fn.fn.tag === 'App' && expr.fn.fn.fn.tag === 'S') {
    const f = expr.fn.fn.arg, g = expr.fn.arg, x = expr.arg;
    return new CombApp(new CombApp(f, x), new CombApp(g, x));
  }
  // Reduce inside
  if (expr.tag === 'App') {
    const fn = combStep(expr.fn);
    if (fn !== expr.fn) return new CombApp(fn, expr.arg);
    const arg = combStep(expr.arg);
    if (arg !== expr.arg) return new CombApp(expr.fn, arg);
  }
  return expr;
}

function combReduce(expr, maxSteps = 100) {
  let current = expr, steps = 0;
  while (steps < maxSteps) {
    const next = combStep(current);
    if (next === current) break;
    current = next; steps++;
  }
  return { result: current, steps };
}

// Lambda → Combinator (bracket abstraction)
function bracket(varName, expr) {
  if (expr.tag === 'Var' && expr.name === varName) return new CombI();
  if (!hasFree(varName, expr)) return new CombApp(new CombK(), expr);
  if (expr.tag === 'App') {
    return new CombApp(new CombApp(new CombS(), bracket(varName, expr.fn)), bracket(varName, expr.arg));
  }
  return expr;
}

function hasFree(name, expr) {
  if (expr.tag === 'Var') return expr.name === name;
  if (expr.tag === 'App') return hasFree(name, expr.fn) || hasFree(name, expr.arg);
  return false;
}

// Size of combinator term
function combSize(expr) {
  if (expr.tag === 'App') return 1 + combSize(expr.fn) + combSize(expr.arg);
  return 1;
}

export { I, K, S, B, C, W, T, CombI, CombK, CombS, CombB, CombC, CombApp, CombVar, combStep, combReduce, bracket, combSize };
