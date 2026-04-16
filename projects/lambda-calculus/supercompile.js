/**
 * Supercompilation: Driving + Folding + Generalization
 * 
 * A powerful program transformation that:
 * 1. Drives: unfolds computation symbolically
 * 2. Folds: detects when a previously seen state recurs (guarantees termination)
 * 3. Generalizes: extracts common structure when folding fails
 * 
 * This is a simplified supercompiler for a small expression language.
 */

class Num { constructor(n) { this.tag = 'Num'; this.n = n; } eq(o) { return o.tag === 'Num' && o.n === this.n; } toString() { return `${this.n}`; } }
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } eq(o) { return o.tag === 'Var' && o.name === this.name; } toString() { return this.name; } }
class Add { constructor(l, r) { this.tag = 'Add'; this.left = l; this.right = r; } eq(o) { return o.tag === 'Add' && this.left.eq(o.left) && this.right.eq(o.right); } toString() { return `(${this.left} + ${this.right})`; } }
class Mul { constructor(l, r) { this.tag = 'Mul'; this.left = l; this.right = r; } eq(o) { return o.tag === 'Mul' && this.left.eq(o.left) && this.right.eq(o.right); } toString() { return `(${this.left} * ${this.right})`; } }
class If0 { constructor(c, t, f) { this.tag = 'If0'; this.cond = c; this.then = t; this.else = f; } eq(o) { return o.tag === 'If0' && this.cond.eq(o.cond); } toString() { return `if0(${this.cond}, ${this.then}, ${this.else})`; } }

// Drive: one step of symbolic evaluation
function drive(expr) {
  switch (expr.tag) {
    case 'Num': case 'Var': return expr;
    case 'Add':
      if (expr.left.tag === 'Num' && expr.right.tag === 'Num') return new Num(expr.left.n + expr.right.n);
      if (expr.left.tag === 'Num' && expr.left.n === 0) return expr.right;
      if (expr.right.tag === 'Num' && expr.right.n === 0) return expr.left;
      return new Add(drive(expr.left), drive(expr.right));
    case 'Mul':
      if (expr.left.tag === 'Num' && expr.right.tag === 'Num') return new Num(expr.left.n * expr.right.n);
      if (expr.left.tag === 'Num' && expr.left.n === 0) return new Num(0);
      if (expr.right.tag === 'Num' && expr.right.n === 0) return new Num(0);
      if (expr.left.tag === 'Num' && expr.left.n === 1) return expr.right;
      if (expr.right.tag === 'Num' && expr.right.n === 1) return expr.left;
      return new Mul(drive(expr.left), drive(expr.right));
    case 'If0':
      if (expr.cond.tag === 'Num') return expr.cond.n === 0 ? expr.then : expr.else;
      return new If0(drive(expr.cond), drive(expr.then), drive(expr.else));
    default: return expr;
  }
}

// Fold: check if expression was seen before (whistle)
function homeomorphicEmbedding(e1, e2) {
  if (e1.tag === e2.tag) {
    if (e1.tag === 'Num') return e1.n <= e2.n;
    if (e1.tag === 'Var') return e1.name === e2.name;
  }
  // Diving: e1 embeds in a subexpression of e2
  if (e2.tag === 'Add') return homeomorphicEmbedding(e1, e2.left) || homeomorphicEmbedding(e1, e2.right);
  if (e2.tag === 'Mul') return homeomorphicEmbedding(e1, e2.left) || homeomorphicEmbedding(e1, e2.right);
  return false;
}

// Supercompile: iterate drive until fixpoint, with folding
function supercompile(expr, maxSteps = 100) {
  const history = [];
  let current = expr;
  
  for (let i = 0; i < maxSteps; i++) {
    // Check whistle (folding)
    if (history.some(h => homeomorphicEmbedding(h, current))) break;
    history.push(current);
    
    const next = drive(current);
    if (next.toString() === current.toString()) break; // Fixpoint
    current = next;
  }
  
  return { result: current, steps: history.length };
}

export { Num, Var, Add, Mul, If0, drive, homeomorphicEmbedding, supercompile };
