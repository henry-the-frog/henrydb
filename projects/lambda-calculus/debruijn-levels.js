/**
 * De Bruijn Levels (alternative to indices)
 * 
 * De Bruijn INDICES: count from the binding site inward (used in lambda.js)
 * De Bruijn LEVELS: count from the outermost binder outward
 * 
 * λ. λ. 1 0   (indices: 1 refers to outer, 0 to inner)
 * λ. λ. 0 1   (levels:  0 refers to outer, 1 to inner)
 * 
 * Levels are better for:
 * - NbE (readback produces levels naturally)
 * - Weakening (levels don't change when adding bindings)
 * - Type checking with open terms
 */

class LVar { constructor(level) { this.tag = 'LVar'; this.level = level; } toString() { return `${this.level}`; } }
class LLam { constructor(body) { this.tag = 'LLam'; this.body = body; } toString() { return `(λ.${this.body})`; } }
class LApp { constructor(fn, arg) { this.tag = 'LApp'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class LNum { constructor(n) { this.tag = 'LNum'; this.n = n; } toString() { return `${this.n}`; } }

// Named terms (for conversion)
class NVar { constructor(name) { this.tag = 'NVar'; this.name = name; } }
class NLam { constructor(v, body) { this.tag = 'NLam'; this.var = v; this.body = body; } }
class NApp { constructor(fn, arg) { this.tag = 'NApp'; this.fn = fn; this.arg = arg; } }
class NNum { constructor(n) { this.tag = 'NNum'; this.n = n; } }

// ============================================================
// Named → Levels
// ============================================================

function namedToLevels(expr, env = new Map(), depth = 0) {
  switch (expr.tag) {
    case 'NNum': return new LNum(expr.n);
    case 'NVar': {
      const level = env.get(expr.name);
      if (level === undefined) throw new Error(`Unbound: ${expr.name}`);
      return new LVar(level);
    }
    case 'NLam': {
      const newEnv = new Map([...env, [expr.var, depth]]);
      return new LLam(namedToLevels(expr.body, newEnv, depth + 1));
    }
    case 'NApp': return new LApp(namedToLevels(expr.fn, env, depth), namedToLevels(expr.arg, env, depth));
  }
}

// ============================================================
// Levels → Named
// ============================================================

function levelsToNamed(expr, depth = 0) {
  switch (expr.tag) {
    case 'LNum': return new NNum(expr.n);
    case 'LVar': return new NVar(`x${expr.level}`);
    case 'LLam': return new NLam(`x${depth}`, levelsToNamed(expr.body, depth + 1));
    case 'LApp': return new NApp(levelsToNamed(expr.fn, depth), levelsToNamed(expr.arg, depth));
  }
}

// ============================================================
// Levels ↔ Indices conversion
// ============================================================

function levelsToIndices(expr, depth = 0) {
  switch (expr.tag) {
    case 'LNum': return new LNum(expr.n);
    case 'LVar': return new LVar(depth - 1 - expr.level);
    case 'LLam': return new LLam(levelsToIndices(expr.body, depth + 1));
    case 'LApp': return new LApp(levelsToIndices(expr.fn, depth), levelsToIndices(expr.arg, depth));
  }
}

function indicesToLevels(expr, depth = 0) {
  switch (expr.tag) {
    case 'LNum': return new LNum(expr.n);
    case 'LVar': return new LVar(depth - 1 - expr.level);
    case 'LLam': return new LLam(indicesToLevels(expr.body, depth + 1));
    case 'LApp': return new LApp(indicesToLevels(expr.fn, depth), indicesToLevels(expr.arg, depth));
  }
}

// ============================================================
// Substitution (levels-based — simpler than indices!)
// ============================================================

function substLevel(expr, level, replacement) {
  switch (expr.tag) {
    case 'LNum': return expr;
    case 'LVar': return expr.level === level ? replacement : expr;
    case 'LLam': return new LLam(substLevel(expr.body, level, replacement));
    case 'LApp': return new LApp(substLevel(expr.fn, level, replacement), substLevel(expr.arg, level, replacement));
  }
}

export {
  LVar, LLam, LApp, LNum, NVar, NLam, NApp, NNum,
  namedToLevels, levelsToNamed, levelsToIndices, indicesToLevels, substLevel
};
