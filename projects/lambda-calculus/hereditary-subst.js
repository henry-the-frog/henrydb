/**
 * Hereditary Substitution: No intermediate redexes
 * 
 * Standard substitution can create new redexes: (λx.(λy.y) x)[z/x] → (λy.y) z (redex!)
 * Hereditary substitution normalizes as it substitutes: no intermediate redexes.
 * 
 * Used in: Logical frameworks (Twelf), NbE implementations.
 */

class Var { constructor(idx) { this.tag = 'Var'; this.idx = idx; } toString() { return `${this.idx}`; } }
class Lam { constructor(body) { this.tag = 'Lam'; this.body = body; } toString() { return `(λ.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }

// Shift: increase free variables by delta
function shift(expr, delta, cutoff = 0) {
  switch (expr.tag) {
    case 'Var': return expr.idx >= cutoff ? new Var(expr.idx + delta) : expr;
    case 'Lam': return new Lam(shift(expr.body, delta, cutoff + 1));
    case 'App': return new App(shift(expr.fn, delta, cutoff), shift(expr.arg, delta, cutoff));
  }
}

// Standard substitution (may create redexes)
function standardSubst(expr, idx, replacement) {
  switch (expr.tag) {
    case 'Var':
      if (expr.idx === idx) return replacement;
      if (expr.idx > idx) return new Var(expr.idx - 1);
      return expr;
    case 'Lam':
      return new Lam(standardSubst(expr.body, idx + 1, shift(replacement, 1)));
    case 'App':
      return new App(standardSubst(expr.fn, idx, replacement), standardSubst(expr.arg, idx, replacement));
  }
}

// Hereditary substitution (normalizes as it goes)
function hereditarySubst(expr, idx, replacement) {
  switch (expr.tag) {
    case 'Var':
      if (expr.idx === idx) return replacement;
      if (expr.idx > idx) return new Var(expr.idx - 1);
      return expr;
    case 'Lam':
      return new Lam(hereditarySubst(expr.body, idx + 1, shift(replacement, 1)));
    case 'App': {
      const fn = hereditarySubst(expr.fn, idx, replacement);
      const arg = hereditarySubst(expr.arg, idx, replacement);
      // KEY: if fn is a lambda, immediately beta-reduce (hereditary!)
      if (fn.tag === 'Lam') return hereditarySubst(fn.body, 0, arg);
      return new App(fn, arg);
    }
  }
}

// Check if term is in normal form (no redexes)
function isNormal(expr) {
  switch (expr.tag) {
    case 'Var': return true;
    case 'Lam': return isNormal(expr.body);
    case 'App': return expr.fn.tag !== 'Lam' && isNormal(expr.fn) && isNormal(expr.arg);
  }
}

// Count redexes
function countRedexes(expr) {
  switch (expr.tag) {
    case 'Var': return 0;
    case 'Lam': return countRedexes(expr.body);
    case 'App': return (expr.fn.tag === 'Lam' ? 1 : 0) + countRedexes(expr.fn) + countRedexes(expr.arg);
  }
}

export { Var, Lam, App, shift, standardSubst, hereditarySubst, isNormal, countRedexes };
