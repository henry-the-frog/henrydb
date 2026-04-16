/**
 * Rewrite Rules: User-defined optimization rules
 * 
 * Like GHC's RULES pragmas: {-# RULES "map/map" map f . map g = map (f . g) #-}
 * Pattern-match on code structure, replace with optimized version.
 */

class Rule {
  constructor(name, pattern, replacement, condition = () => true) {
    this.name = name;
    this.pattern = pattern;         // (expr) → bindings | null
    this.replacement = replacement; // (bindings) → expr
    this.condition = condition;
    this.fireCount = 0;
  }
}

// Expression types
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class BinOp { constructor(op, left, right) { this.tag = 'BinOp'; this.op = op; this.left = left; this.right = right; } toString() { return `(${this.left} ${this.op} ${this.right})`; } }

class RuleEngine {
  constructor() { this.rules = []; }
  
  addRule(rule) { this.rules.push(rule); return this; }
  
  applyOnce(expr) {
    for (const rule of this.rules) {
      const bindings = rule.pattern(expr);
      if (bindings && rule.condition(bindings)) {
        rule.fireCount++;
        return { fired: rule.name, result: rule.replacement(bindings) };
      }
    }
    return null;
  }
  
  rewrite(expr, maxSteps = 100) {
    let current = expr, steps = 0, fired = [];
    while (steps < maxSteps) {
      const result = this.applyDeep(current);
      if (!result) break;
      current = result.result;
      fired.push(result.fired);
      steps++;
    }
    return { result: current, steps, fired };
  }
  
  applyDeep(expr) {
    const direct = this.applyOnce(expr);
    if (direct) return direct;
    
    if (expr.tag === 'App') {
      const fn = this.applyDeep(expr.fn);
      if (fn) return { fired: fn.fired, result: new App(fn.result, expr.arg) };
      const arg = this.applyDeep(expr.arg);
      if (arg) return { fired: arg.fired, result: new App(expr.fn, arg.result) };
    }
    if (expr.tag === 'Lam') {
      const body = this.applyDeep(expr.body);
      if (body) return { fired: body.fired, result: new Lam(expr.var, body.result) };
    }
    if (expr.tag === 'BinOp') {
      const l = this.applyDeep(expr.left);
      if (l) return { fired: l.fired, result: new BinOp(expr.op, l.result, expr.right) };
      const r = this.applyDeep(expr.right);
      if (r) return { fired: r.fired, result: new BinOp(expr.op, expr.left, r.result) };
    }
    return null;
  }
  
  stats() { return this.rules.map(r => ({ name: r.name, fires: r.fireCount })); }
}

// Standard rules
const doubleNeg = new Rule('double-neg',
  e => e.tag === 'App' && e.fn.tag === 'Var' && e.fn.name === 'neg' && e.arg.tag === 'App' && e.arg.fn.tag === 'Var' && e.arg.fn.name === 'neg' ? { x: e.arg.arg } : null,
  b => b.x
);

const addZero = new Rule('add-zero',
  e => e.tag === 'BinOp' && e.op === '+' && e.right.tag === 'Num' && e.right.n === 0 ? { x: e.left } : null,
  b => b.x
);

const mulOne = new Rule('mul-one',
  e => e.tag === 'BinOp' && e.op === '*' && e.right.tag === 'Num' && e.right.n === 1 ? { x: e.left } : null,
  b => b.x
);

const constFold = new Rule('const-fold',
  e => {
    if (e.tag !== 'BinOp' || e.left.tag !== 'Num' || e.right.tag !== 'Num') return null;
    return { op: e.op, a: e.left.n, b: e.right.n };
  },
  b => {
    switch (b.op) { case '+': return new Num(b.a + b.b); case '*': return new Num(b.a * b.b); case '-': return new Num(b.a - b.b); default: return new Num(0); }
  }
);

export { Rule, RuleEngine, Var, Num, App, Lam, BinOp, doubleNeg, addZero, mulOne, constFold };
