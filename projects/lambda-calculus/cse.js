/**
 * Common Subexpression Elimination (CSE) for lambda calculus
 * 
 * Detect identical subexpressions and share them via let bindings.
 * If `f(x) + f(x)` occurs, transform to `let t = f(x) in t + t`.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class BinOp { constructor(op, l, r) { this.tag = 'BinOp'; this.op = op; this.left = l; this.right = r; } toString() { return `(${this.left} ${this.op} ${this.right})`; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } toString() { return `(let ${this.var} = ${this.init} in ${this.body})`; } }

function exprKey(expr) {
  switch (expr.tag) {
    case 'Var': return `v:${expr.name}`;
    case 'Num': return `n:${expr.n}`;
    case 'App': return `a:${exprKey(expr.fn)}|${exprKey(expr.arg)}`;
    case 'BinOp': return `b:${expr.op}:${exprKey(expr.left)}|${exprKey(expr.right)}`;
    default: return `?:${expr.tag}`;
  }
}

function collectSubexprs(expr, counts = new Map()) {
  const key = exprKey(expr);
  counts.set(key, (counts.get(key) || 0) + 1);
  switch (expr.tag) {
    case 'App': collectSubexprs(expr.fn, counts); collectSubexprs(expr.arg, counts); break;
    case 'BinOp': collectSubexprs(expr.left, counts); collectSubexprs(expr.right, counts); break;
  }
  return counts;
}

function findCSE(expr) {
  const counts = collectSubexprs(expr);
  const duplicates = [];
  for (const [key, count] of counts) {
    if (count > 1 && !key.startsWith('v:') && !key.startsWith('n:')) {
      duplicates.push(key);
    }
  }
  return duplicates;
}

let cseCounter = 0;
function freshCSE() { return `_cse${cseCounter++}`; }
function resetCSE() { cseCounter = 0; }

function eliminateCSE(expr) {
  resetCSE();
  const counts = collectSubexprs(expr);
  const toReplace = new Map();
  
  // Find expressions that appear more than once and are non-trivial
  for (const [key, count] of counts) {
    if (count > 1 && !key.startsWith('v:') && !key.startsWith('n:')) {
      toReplace.set(key, freshCSE());
    }
  }
  
  if (toReplace.size === 0) return expr;
  
  // Replace duplicate subexpressions with variables
  const replaced = replaceCSE(expr, toReplace);
  
  // Wrap in let bindings
  let result = replaced;
  for (const [key, varName] of toReplace) {
    const original = findExprByKey(expr, key);
    if (original) result = new Let(varName, original, result);
  }
  
  return result;
}

function replaceCSE(expr, toReplace) {
  const key = exprKey(expr);
  if (toReplace.has(key)) return new Var(toReplace.get(key));
  switch (expr.tag) {
    case 'App': return new App(replaceCSE(expr.fn, toReplace), replaceCSE(expr.arg, toReplace));
    case 'BinOp': return new BinOp(expr.op, replaceCSE(expr.left, toReplace), replaceCSE(expr.right, toReplace));
    default: return expr;
  }
}

function findExprByKey(expr, targetKey) {
  if (exprKey(expr) === targetKey) return expr;
  switch (expr.tag) {
    case 'App': return findExprByKey(expr.fn, targetKey) || findExprByKey(expr.arg, targetKey);
    case 'BinOp': return findExprByKey(expr.left, targetKey) || findExprByKey(expr.right, targetKey);
    default: return null;
  }
}

export { Var, Num, App, BinOp, Let, exprKey, collectSubexprs, findCSE, eliminateCSE, resetCSE };
