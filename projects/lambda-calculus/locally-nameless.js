/**
 * Locally Nameless Representation
 * 
 * Bound variables: de Bruijn indices (0, 1, ...)
 * Free variables: names ("x", "y", ...)
 * 
 * Best of both worlds:
 * - No α-conversion needed (bound vars are indices)
 * - Free vars are human-readable (named)
 * - Opening: replace bound var with free name
 * - Closing: replace free name with bound index
 */

class BVar { constructor(idx) { this.tag = 'BVar'; this.idx = idx; } toString() { return `b${this.idx}`; } }
class FVar { constructor(name) { this.tag = 'FVar'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(body) { this.tag = 'Lam'; this.body = body; } toString() { return `(λ.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }

// ============================================================
// Open: replace bound var k with free var name
// ============================================================

function open(expr, k, name) {
  switch (expr.tag) {
    case 'BVar': return expr.idx === k ? new FVar(name) : expr;
    case 'FVar': return expr;
    case 'Num': return expr;
    case 'Lam': return new Lam(open(expr.body, k + 1, name));
    case 'App': return new App(open(expr.fn, k, name), open(expr.arg, k, name));
  }
}

// Open at level 0 (most common)
function openTerm(expr, name) { return open(expr, 0, name); }

// ============================================================
// Close: replace free var name with bound var k
// ============================================================

function close(expr, k, name) {
  switch (expr.tag) {
    case 'BVar': return expr;
    case 'FVar': return expr.name === name ? new BVar(k) : expr;
    case 'Num': return expr;
    case 'Lam': return new Lam(close(expr.body, k + 1, name));
    case 'App': return new App(close(expr.fn, k, name), close(expr.arg, k, name));
  }
}

function closeTerm(expr, name) { return close(expr, 0, name); }

// ============================================================
// Free variables
// ============================================================

function freeVars(expr) {
  switch (expr.tag) {
    case 'BVar': return new Set();
    case 'FVar': return new Set([expr.name]);
    case 'Num': return new Set();
    case 'Lam': return freeVars(expr.body);
    case 'App': return new Set([...freeVars(expr.fn), ...freeVars(expr.arg)]);
  }
}

// ============================================================
// Substitution: replace free var with term
// ============================================================

function subst(expr, name, replacement) {
  switch (expr.tag) {
    case 'BVar': return expr;
    case 'FVar': return expr.name === name ? replacement : expr;
    case 'Num': return expr;
    case 'Lam': return new Lam(subst(expr.body, name, replacement));
    case 'App': return new App(subst(expr.fn, name, replacement), subst(expr.arg, name, replacement));
  }
}

// ============================================================
// Local closure: open with fresh name, process, close back
// ============================================================

let freshCounter = 0;
function fresh() { return `_x${freshCounter++}`; }
function resetFresh() { freshCounter = 0; }

function withOpen(lamBody, fn) {
  const x = fresh();
  const opened = openTerm(lamBody, x);
  const result = fn(x, opened);
  return closeTerm(result, x);
}

// ============================================================
// Well-formedness: no dangling bound variables
// ============================================================

function isLocallyClosed(expr, depth = 0) {
  switch (expr.tag) {
    case 'BVar': return expr.idx < depth;
    case 'FVar': case 'Num': return true;
    case 'Lam': return isLocallyClosed(expr.body, depth + 1);
    case 'App': return isLocallyClosed(expr.fn, depth) && isLocallyClosed(expr.arg, depth);
  }
}

export {
  BVar, FVar, Lam, App, Num,
  openTerm, closeTerm, freeVars, subst, withOpen,
  fresh, resetFresh, isLocallyClosed
};
