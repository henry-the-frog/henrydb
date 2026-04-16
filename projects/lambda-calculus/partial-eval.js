/**
 * Partial Evaluation: Specialize programs at compile time
 * 
 * Given a function f(x, y) and a known value for x,
 * produce a specialized function f_x(y) that's faster.
 * 
 * Also known as: Futamura projections, program specialization.
 */

class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Add { constructor(l, r) { this.tag = 'Add'; this.left = l; this.right = r; } toString() { return `(${this.left} + ${this.right})`; } }
class Mul { constructor(l, r) { this.tag = 'Mul'; this.left = l; this.right = r; } toString() { return `(${this.left} * ${this.right})`; } }
class If0 { constructor(c, t, f) { this.tag = 'If0'; this.cond = c; this.then = t; this.else = f; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }

// Static = known at compile time, Dynamic = known only at runtime
const STATIC = 'static';
const DYNAMIC = 'dynamic';

function partialEval(expr, env = new Map()) {
  switch (expr.tag) {
    case 'Num': return expr;
    case 'Var': return env.has(expr.name) ? env.get(expr.name) : expr;
    case 'Add': {
      const l = partialEval(expr.left, env);
      const r = partialEval(expr.right, env);
      if (l.tag === 'Num' && r.tag === 'Num') return new Num(l.n + r.n);
      if (l.tag === 'Num' && l.n === 0) return r;
      if (r.tag === 'Num' && r.n === 0) return l;
      return new Add(l, r);
    }
    case 'Mul': {
      const l = partialEval(expr.left, env);
      const r = partialEval(expr.right, env);
      if (l.tag === 'Num' && r.tag === 'Num') return new Num(l.n * r.n);
      if (l.tag === 'Num' && l.n === 0) return new Num(0);
      if (r.tag === 'Num' && r.n === 0) return new Num(0);
      if (l.tag === 'Num' && l.n === 1) return r;
      if (r.tag === 'Num' && r.n === 1) return l;
      return new Mul(l, r);
    }
    case 'If0': {
      const c = partialEval(expr.cond, env);
      if (c.tag === 'Num') return partialEval(c.n === 0 ? expr.then : expr.else, env);
      return new If0(c, partialEval(expr.then, env), partialEval(expr.else, env));
    }
    case 'Let': {
      const init = partialEval(expr.init, env);
      const newEnv = new Map([...env, [expr.var, init]]);
      return partialEval(expr.body, newEnv);
    }
    default: return expr;
  }
}

// Specialize: given static params, produce residual code
function specialize(expr, staticParams) {
  const env = new Map(Object.entries(staticParams).map(([k, v]) => [k, new Num(v)]));
  return partialEval(expr, env);
}

// Count operations in residual code
function countOps(expr) {
  switch (expr.tag) {
    case 'Num': case 'Var': return 0;
    case 'Add': case 'Mul': return 1 + countOps(expr.left) + countOps(expr.right);
    case 'If0': return 1 + countOps(expr.cond) + countOps(expr.then) + countOps(expr.else);
    case 'Let': return countOps(expr.init) + countOps(expr.body);
    default: return 0;
  }
}

export { Num, Var, Add, Mul, If0, Let, partialEval, specialize, countOps };
