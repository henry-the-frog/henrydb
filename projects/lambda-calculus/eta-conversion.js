/**
 * MODULE #100: Eta Conversion & Extensionality
 * 
 * The three fundamental operations of lambda calculus:
 * 1. α (alpha): rename bound variables
 * 2. β (beta): function application ((λx.M) N → M[x:=N])
 * 3. η (eta): extensionality (λx.f x → f when x ∉ FV(f))
 * 
 * Eta conversion captures the idea that two functions are equal
 * if they produce the same output for every input (extensionality).
 * 
 * η-expansion: f → λx.f x (wrap in lambda)
 * η-reduction: λx.f x → f (unwrap if possible)
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }

function freeVars(expr) {
  switch (expr.tag) {
    case 'Var': return new Set([expr.name]);
    case 'Num': return new Set();
    case 'Lam': { const fv = freeVars(expr.body); fv.delete(expr.var); return fv; }
    case 'App': return new Set([...freeVars(expr.fn), ...freeVars(expr.arg)]);
    default: return new Set();
  }
}

/**
 * η-reducible: λx.f x where x ∉ FV(f)
 */
function isEtaReducible(expr) {
  if (expr.tag !== 'Lam') return false;
  if (expr.body.tag !== 'App') return false;
  if (expr.body.arg.tag !== 'Var') return false;
  if (expr.body.arg.name !== expr.var) return false;
  if (freeVars(expr.body.fn).has(expr.var)) return false;
  return true;
}

/**
 * η-reduce: λx.f x → f
 */
function etaReduce(expr) {
  if (!isEtaReducible(expr)) return expr;
  return expr.body.fn;
}

/**
 * η-expand: f → λx.f x (with fresh variable)
 */
let freshCounter = 0;
function fresh() { return `η${freshCounter++}`; }
function resetFresh() { freshCounter = 0; }

function etaExpand(expr) {
  const x = fresh();
  return new Lam(x, new App(expr, new Var(x)));
}

/**
 * Deep η-reduce: reduce everywhere in the term
 */
function deepEtaReduce(expr) {
  switch (expr.tag) {
    case 'Var': case 'Num': return expr;
    case 'Lam': {
      const body = deepEtaReduce(expr.body);
      const reduced = new Lam(expr.var, body);
      return isEtaReducible(reduced) ? etaReduce(reduced) : reduced;
    }
    case 'App': return new App(deepEtaReduce(expr.fn), deepEtaReduce(expr.arg));
    default: return expr;
  }
}

/**
 * βη-equality: check if two terms are equal up to β and η
 */
function betaEtaEqual(a, b) {
  const na = deepEtaReduce(a);
  const nb = deepEtaReduce(b);
  return structuralEqual(na, nb);
}

function structuralEqual(a, b) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'Var': return a.name === b.name;
    case 'Num': return a.n === b.n;
    case 'Lam': return a.var === b.var && structuralEqual(a.body, b.body);
    case 'App': return structuralEqual(a.fn, b.fn) && structuralEqual(a.arg, b.arg);
    default: return false;
  }
}

/**
 * Extensionality: two functions f, g are extensionally equal if
 * for all x, f(x) = g(x)
 */
function testExtensionality(f, g, inputs) {
  return inputs.every(x => {
    try { return f(x) === g(x); }
    catch { return false; }
  });
}

export {
  Var, Lam, App, Num, freeVars,
  isEtaReducible, etaReduce, etaExpand,
  deepEtaReduce, betaEtaEqual, structuralEqual,
  testExtensionality, resetFresh
};
