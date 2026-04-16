/**
 * Explicit Substitutions (λσ-calculus)
 * 
 * Instead of performing substitution immediately, represent it as a first-class operation.
 * M[σ] means "M with substitution σ pending"
 * 
 * Rules:
 * - (λ.M)[σ] → λ.(M[↑σ])      (extend substitution under binder)
 * - (M N)[σ] → (M[σ])(N[σ])    (distribute over application)
 * - 0[M·σ] → M                  (lookup variable 0)
 * - (n+1)[M·σ] → n[σ]           (shift index)
 * - n[↑] → n+1                  (shift)
 */

class Idx { constructor(n) { this.tag = 'Idx'; this.n = n; } toString() { return `${this.n}`; } }
class Lam { constructor(body) { this.tag = 'Lam'; this.body = body; } toString() { return `(λ.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Clos { constructor(term, subst) { this.tag = 'Clos'; this.term = term; this.subst = subst; } toString() { return `${this.term}[${this.subst}]`; } }

// Substitutions
class Cons { constructor(head, tail) { this.tag = 'Cons'; this.head = head; this.tail = tail; } toString() { return `${this.head}·${this.tail}`; } }
class Shift { constructor(n = 1) { this.tag = 'Shift'; this.n = n; } toString() { return `↑${this.n > 1 ? this.n : ''}`; } }
class Id { constructor() { this.tag = 'Id'; } toString() { return 'id'; } }

const id = new Id();
const shift = new Shift();

// Compose substitution: σ ∘ τ
function compose(s1, s2) {
  if (s1.tag === 'Id') return s2;
  if (s2.tag === 'Id') return s1;
  return { tag: 'Compose', left: s1, right: s2 };
}

// Step-by-step reduction
function step(expr) {
  if (expr.tag === 'Clos') {
    const { term, subst } = expr;
    
    // Variable lookup
    if (term.tag === 'Idx') {
      if (subst.tag === 'Id') return term;
      if (subst.tag === 'Shift') return new Idx(term.n + subst.n);
      if (subst.tag === 'Cons') {
        if (term.n === 0) return subst.head;
        return new Clos(new Idx(term.n - 1), subst.tail);
      }
    }
    
    // App distributes
    if (term.tag === 'App') {
      return new App(new Clos(term.fn, subst), new Clos(term.arg, subst));
    }
    
    // Lambda extends
    if (term.tag === 'Lam') {
      return new Lam(new Clos(term.body, new Cons(new Idx(0), compose(subst, shift))));
    }
  }
  
  // Reduce inside
  if (expr.tag === 'App') {
    // Beta reduction
    if (expr.fn.tag === 'Lam') {
      return new Clos(expr.fn.body, new Cons(expr.arg, id));
    }
    const fn = step(expr.fn);
    if (fn !== expr.fn) return new App(fn, expr.arg);
    const arg = step(expr.arg);
    if (arg !== expr.arg) return new App(expr.fn, arg);
  }
  
  if (expr.tag === 'Lam') {
    const body = step(expr.body);
    if (body !== expr.body) return new Lam(body);
  }
  
  return expr;
}

function reduce(expr, maxSteps = 100) {
  let current = expr, steps = 0;
  while (steps < maxSteps) {
    const next = step(current);
    if (next === current) break;
    current = next; steps++;
  }
  return { result: current, steps };
}

export { Idx, Lam, App, Clos, Cons, Shift, Id, id, shift, step, reduce };
