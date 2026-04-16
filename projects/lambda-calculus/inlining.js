/**
 * Inlining Heuristics: When to inline function calls
 * 
 * Inline = replace f(x) with body of f. Always safe but:
 * - Can cause code bloat (body duplicated everywhere)
 * - Enables further optimizations (constant folding after inline)
 * 
 * Heuristics: inline if small, single-use, or enables known optimization.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }
class BinOp { constructor(op, l, r) { this.tag = 'BinOp'; this.op = op; this.left = l; this.right = r; } }

function exprSize(expr) {
  switch (expr.tag) {
    case 'Var': case 'Num': return 1;
    case 'Lam': return 1 + exprSize(expr.body);
    case 'App': return 1 + exprSize(expr.fn) + exprSize(expr.arg);
    case 'Let': return 1 + exprSize(expr.init) + exprSize(expr.body);
    case 'BinOp': return 1 + exprSize(expr.left) + exprSize(expr.right);
    default: return 1;
  }
}

function usageCount(expr, varName) {
  switch (expr.tag) {
    case 'Var': return expr.name === varName ? 1 : 0;
    case 'Num': return 0;
    case 'Lam': return usageCount(expr.body, varName);
    case 'App': return usageCount(expr.fn, varName) + usageCount(expr.arg, varName);
    case 'Let': return usageCount(expr.init, varName) + usageCount(expr.body, varName);
    case 'BinOp': return usageCount(expr.left, varName) + usageCount(expr.right, varName);
    default: return 0;
  }
}

const SMALL_THRESHOLD = 5;    // Inline if body smaller than this
const INLINE_ALWAYS = 'always';
const INLINE_NEVER = 'never';
const INLINE_MAYBE = 'maybe';

function shouldInline(varName, init, body, opts = {}) {
  const threshold = opts.threshold || SMALL_THRESHOLD;
  const uses = usageCount(body, varName);
  const size = exprSize(init);
  
  // Dead: always inline (will be removed)
  if (uses === 0) return { decision: INLINE_ALWAYS, reason: 'dead code' };
  
  // Single use: always inline (no duplication)
  if (uses === 1) return { decision: INLINE_ALWAYS, reason: 'single use' };
  
  // Trivial (variable or number): always inline
  if (init.tag === 'Var' || init.tag === 'Num') return { decision: INLINE_ALWAYS, reason: 'trivial' };
  
  // Small: inline if not too many uses
  if (size <= threshold && uses * size < threshold * 3) return { decision: INLINE_MAYBE, reason: `small (${size} nodes, ${uses} uses)` };
  
  // Large with many uses: don't inline
  return { decision: INLINE_NEVER, reason: `too large (${size} nodes, ${uses} uses)` };
}

function subst(expr, name, repl) {
  switch (expr.tag) {
    case 'Var': return expr.name === name ? repl : expr;
    case 'Num': return expr;
    case 'Lam': return expr.var === name ? expr : new Lam(expr.var, subst(expr.body, name, repl));
    case 'App': return new App(subst(expr.fn, name, repl), subst(expr.arg, name, repl));
    case 'Let': return new Let(expr.var, subst(expr.init, name, repl), expr.var === name ? expr.body : subst(expr.body, name, repl));
    case 'BinOp': return new BinOp(expr.op, subst(expr.left, name, repl), subst(expr.right, name, repl));
    default: return expr;
  }
}

function applyInlining(expr) {
  if (expr.tag !== 'Let') {
    switch (expr.tag) {
      case 'Lam': return new Lam(expr.var, applyInlining(expr.body));
      case 'App': return new App(applyInlining(expr.fn), applyInlining(expr.arg));
      case 'BinOp': return new BinOp(expr.op, applyInlining(expr.left), applyInlining(expr.right));
      default: return expr;
    }
  }
  
  const init = applyInlining(expr.init);
  const body = applyInlining(expr.body);
  const decision = shouldInline(expr.var, init, body);
  
  if (decision.decision === INLINE_ALWAYS) return subst(body, expr.var, init);
  return new Let(expr.var, init, body);
}

export { Var, Num, Lam, App, Let, BinOp, exprSize, usageCount, shouldInline, applyInlining, INLINE_ALWAYS, INLINE_NEVER, INLINE_MAYBE };
