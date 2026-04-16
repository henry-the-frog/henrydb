/**
 * Evaluation Contexts: One-hole contexts for reduction
 * 
 * E[·] represents a term with a "hole" where reduction happens.
 * Used to define evaluation order formally.
 */

class Hole { constructor() { this.tag = 'Hole'; } toString() { return '□'; } }
class EApp1 { constructor(ctx, arg) { this.tag = 'EApp1'; this.ctx = ctx; this.arg = arg; } toString() { return `(${this.ctx} ${this.arg})`; } }
class EApp2 { constructor(val, ctx) { this.tag = 'EApp2'; this.val = val; this.ctx = ctx; } toString() { return `(${this.val} ${this.ctx})`; } }
class EAdd1 { constructor(ctx, right) { this.tag = 'EAdd1'; this.ctx = ctx; this.right = right; } toString() { return `(${this.ctx} + ${this.right})`; } }
class EAdd2 { constructor(val, ctx) { this.tag = 'EAdd2'; this.val = val; this.ctx = ctx; } toString() { return `(${this.val} + ${this.ctx})`; } }

// Plug a term into the hole
function plug(ctx, term) {
  switch (ctx.tag) {
    case 'Hole': return term;
    case 'EApp1': return { tag: 'App', fn: plug(ctx.ctx, term), arg: ctx.arg };
    case 'EApp2': return { tag: 'App', fn: ctx.val, arg: plug(ctx.ctx, term) };
    case 'EAdd1': return { tag: 'Add', left: plug(ctx.ctx, term), right: ctx.right };
    case 'EAdd2': return { tag: 'Add', left: ctx.val, right: plug(ctx.ctx, term) };
  }
}

// Decompose: find the innermost redex and its context
function decompose(expr) {
  if (!expr || typeof expr !== 'object') return null;
  
  if (expr.tag === 'App' && expr.fn.tag === 'Lam') {
    return { ctx: new Hole(), redex: expr };
  }
  if (expr.tag === 'Add' && typeof expr.left === 'number' && typeof expr.right === 'number') {
    return { ctx: new Hole(), redex: expr };
  }
  
  // CBV: evaluate function first, then argument
  if (expr.tag === 'App') {
    const fnDecomp = decompose(expr.fn);
    if (fnDecomp) return { ctx: new EApp1(fnDecomp.ctx, expr.arg), redex: fnDecomp.redex };
    const argDecomp = decompose(expr.arg);
    if (argDecomp) return { ctx: new EApp2(expr.fn, argDecomp.ctx), redex: argDecomp.redex };
  }
  if (expr.tag === 'Add') {
    const leftDecomp = decompose(expr.left);
    if (leftDecomp) return { ctx: new EAdd1(leftDecomp.ctx, expr.right), redex: leftDecomp.redex };
    const rightDecomp = decompose(expr.right);
    if (rightDecomp) return { ctx: new EAdd2(expr.left, rightDecomp.ctx), redex: rightDecomp.redex };
  }
  
  return null;
}

// Is a value?
function isValue(expr) {
  if (typeof expr === 'number') return true;
  if (expr && expr.tag === 'Lam') return true;
  return false;
}

// Context depth
function depth(ctx) {
  switch (ctx.tag) {
    case 'Hole': return 0;
    case 'EApp1': return 1 + depth(ctx.ctx);
    case 'EApp2': return 1 + depth(ctx.ctx);
    case 'EAdd1': return 1 + depth(ctx.ctx);
    case 'EAdd2': return 1 + depth(ctx.ctx);
  }
}

export { Hole, EApp1, EApp2, EAdd1, EAdd2, plug, decompose, isValue, depth };
