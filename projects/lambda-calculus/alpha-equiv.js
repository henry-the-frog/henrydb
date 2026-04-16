/**
 * Alpha Equivalence: Equality up to renaming
 * λx.x ≡α λy.y (same structure, different variable names)
 */
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }

function alphaEq(a, b, envA = new Map(), envB = new Map(), depth = 0) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'Num': return a.n === b.n;
    case 'Var': {
      const da = envA.has(a.name) ? envA.get(a.name) : a.name;
      const db = envB.has(b.name) ? envB.get(b.name) : b.name;
      return da === db;
    }
    case 'Lam':
      return alphaEq(a.body, b.body,
        new Map([...envA, [a.var, depth]]),
        new Map([...envB, [b.var, depth]]), depth + 1);
    case 'App':
      return alphaEq(a.fn, b.fn, envA, envB, depth) &&
             alphaEq(a.arg, b.arg, envA, envB, depth);
  }
}

// Canonical form: rename all bound vars to sequential names
let canonCounter = 0;
function canonicalize(expr, env = new Map()) {
  switch (expr.tag) {
    case 'Num': return expr;
    case 'Var': return env.has(expr.name) ? new Var(env.get(expr.name)) : expr;
    case 'Lam': {
      const canonical = `_${canonCounter++}`;
      const newEnv = new Map([...env, [expr.var, canonical]]);
      return new Lam(canonical, canonicalize(expr.body, newEnv));
    }
    case 'App': return new App(canonicalize(expr.fn, env), canonicalize(expr.arg, env));
  }
}

function toCanonical(expr) { canonCounter = 0; return canonicalize(expr); }
function canonicalString(expr) { return exprToString(toCanonical(expr)); }

function exprToString(expr) {
  switch (expr.tag) {
    case 'Num': return `${expr.n}`;
    case 'Var': return expr.name;
    case 'Lam': return `(λ${expr.var}.${exprToString(expr.body)})`;
    case 'App': return `(${exprToString(expr.fn)} ${exprToString(expr.arg)})`;
  }
}

export { Var, Lam, App, Num, alphaEq, toCanonical, canonicalString, exprToString };
