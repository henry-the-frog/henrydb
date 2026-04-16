/**
 * Strictness Analysis: Determine which arguments are definitely evaluated
 * 
 * f is strict in x if f ⊥ = ⊥ (if x diverges, f diverges)
 * Strict arguments can be evaluated eagerly (no thunk needed) → optimization!
 */

const BOT = Symbol('⊥');

// Abstract domain: {⊥, T} (bottom or top)
function isStrict(f, argIdx, testValues = [0, 1, -1, 42, 100]) {
  // A function is strict in arg i if f(⊥) = ⊥ when ⊥ is in position i
  try {
    const args = testValues.slice(0, argIdx).concat([BOT]).concat(testValues.slice(argIdx + 1));
    f(...args);
    return false; // Didn't throw/diverge → not strict
  } catch {
    return true; // Threw → strict (in practice, ⊥ would cause divergence)
  }
}

// AST-based strictness analysis
class SVar { constructor(name) { this.tag = 'SVar'; this.name = name; } }
class SNum { constructor(n) { this.tag = 'SNum'; this.n = n; } }
class SApp { constructor(fn, arg) { this.tag = 'SApp'; this.fn = fn; this.arg = arg; } }
class SLam { constructor(v, body) { this.tag = 'SLam'; this.var = v; this.body = body; } }
class SIf0 { constructor(c, t, f) { this.tag = 'SIf0'; this.cond = c; this.then = t; this.else = f; } }
class SAdd { constructor(l, r) { this.tag = 'SAdd'; this.left = l; this.right = r; } }
class SSeq { constructor(a, b) { this.tag = 'SSeq'; this.first = a; this.second = b; } }

function analyzeStrictness(expr, varName) {
  // Does expr definitely evaluate varName?
  switch (expr.tag) {
    case 'SVar': return expr.name === varName;
    case 'SNum': return false;
    case 'SAdd': return analyzeStrictness(expr.left, varName) || analyzeStrictness(expr.right, varName);
    case 'SApp': return analyzeStrictness(expr.fn, varName);
    case 'SIf0': return analyzeStrictness(expr.cond, varName); // Only cond is always evaluated
    case 'SSeq': return analyzeStrictness(expr.first, varName) || analyzeStrictness(expr.second, varName);
    case 'SLam': return false; // Body not evaluated yet
    default: return false;
  }
}

// Demand types
const ABSENT = 'absent';     // Never used
const LAZY = 'lazy';         // Used but not immediately
const STRICT = 'strict';     // Definitely evaluated
const HYPERSTRICT = 'hyperstrict'; // Evaluated and fully consumed

function demandType(expr, varName) {
  if (!mentions(expr, varName)) return ABSENT;
  if (analyzeStrictness(expr, varName)) return STRICT;
  return LAZY;
}

function mentions(expr, varName) {
  switch (expr.tag) {
    case 'SVar': return expr.name === varName;
    case 'SNum': return false;
    default: {
      for (const key of Object.keys(expr)) {
        if (key === 'tag') continue;
        if (expr[key] && typeof expr[key] === 'object' && expr[key].tag && mentions(expr[key], varName)) return true;
      }
      return false;
    }
  }
}

export { BOT, isStrict, SVar, SNum, SApp, SLam, SIf0, SAdd, SSeq, analyzeStrictness, demandType, ABSENT, LAZY, STRICT };
