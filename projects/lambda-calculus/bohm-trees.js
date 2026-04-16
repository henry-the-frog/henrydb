/**
 * Böhm Trees: (Possibly Infinite) Normal Forms
 * 
 * A Böhm tree approximates the "meaning" of a lambda term:
 * - If M has a head normal form λx₁...xₙ.y M₁...Mₖ, the root is y
 *   with children being the Böhm trees of M₁...Mₖ
 * - If M has no HNF (diverges), the tree is ⊥
 * 
 * Böhm trees are the basis of denotational semantics of lambda calculus.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }

// Böhm tree nodes
class BT_Bot { constructor() { this.tag = 'BT_Bot'; } toString() { return '⊥'; } }
class BT_Lam { constructor(v, body) { this.tag = 'BT_Lam'; this.var = v; this.body = body; } toString() { return `λ${this.var}.${this.body}`; } }
class BT_Node { constructor(head, args) { this.tag = 'BT_Node'; this.head = head; this.args = args; } toString() { return this.args.length ? `(${this.head} ${this.args.join(' ')})` : this.head; } }

const bot = new BT_Bot();

// Head normal form: λx₁...xₙ.y M₁...Mₖ
function toHNF(expr, maxSteps = 100) {
  let current = expr;
  let steps = 0;
  
  while (steps < maxSteps) {
    if (current.tag === 'Var') return current;
    if (current.tag === 'Lam') return current;
    
    if (current.tag === 'App') {
      // Try to reduce the head to a lambda
      if (current.fn.tag === 'Lam') {
        current = subst(current.fn.body, current.fn.var, current.arg);
        steps++;
        continue;
      }
      // Reduce the head position
      const headReduced = toHNF(current.fn, maxSteps - steps);
      if (headReduced && headReduced !== current.fn) {
        current = new App(headReduced, current.arg);
        continue;
      }
      return current; // Head is stuck (free variable application)
    }
    
    return current;
  }
  
  return null;
}

function subst(expr, name, repl) {
  switch (expr.tag) {
    case 'Var': return expr.name === name ? repl : expr;
    case 'Lam': return expr.var === name ? expr : new Lam(expr.var, subst(expr.body, name, repl));
    case 'App': return new App(subst(expr.fn, name, repl), subst(expr.arg, name, repl));
  }
}

// Build Böhm tree (finite approximation)
function bohmTree(expr, depth = 5) {
  if (depth <= 0) return bot;
  
  const hnf = toHNF(expr);
  if (!hnf) return bot; // Diverges
  
  if (hnf.tag === 'Lam') {
    return new BT_Lam(hnf.var, bohmTree(hnf.body, depth - 1));
  }
  
  // Collect head and arguments
  let head = hnf;
  const args = [];
  while (head.tag === 'App') {
    args.unshift(head.arg);
    head = head.fn;
  }
  
  if (head.tag === 'Var') {
    return new BT_Node(head.name, args.map(a => bohmTree(a, depth - 1)));
  }
  
  return bot;
}

// Finite approximation: truncate at given depth
function approximate(bt, depth) {
  if (depth <= 0) return bot;
  switch (bt.tag) {
    case 'BT_Bot': return bot;
    case 'BT_Lam': return new BT_Lam(bt.var, approximate(bt.body, depth - 1));
    case 'BT_Node': return new BT_Node(bt.head, bt.args.map(a => approximate(a, depth - 1)));
  }
}

// Size of Böhm tree (finite approximation)
function btSize(bt) {
  switch (bt.tag) {
    case 'BT_Bot': return 1;
    case 'BT_Lam': return 1 + btSize(bt.body);
    case 'BT_Node': return 1 + bt.args.reduce((s, a) => s + btSize(a), 0);
  }
}

export { Var, Lam, App, BT_Bot, BT_Lam, BT_Node, bot, toHNF, bohmTree, approximate, btSize };
