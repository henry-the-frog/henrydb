/**
 * Type Holes: Leave blanks in types, fill them later
 * 
 * Like Agda's ? or GHC's _ in types. The type checker tells you
 * what type should go in the hole.
 */

class THole { constructor(id) { this.tag = 'THole'; this.id = id; this.solution = null; } toString() { return this.solution ? this.solution.toString() : `?${this.id}`; } }
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TCon { constructor(name) { this.tag = 'TCon'; this.name = name; } toString() { return this.name; } }

class HoleContext {
  constructor() { this.holes = new Map(); this.nextId = 0; }
  
  createHole(context = null) {
    const id = this.nextId++;
    const hole = new THole(id);
    this.holes.set(id, { hole, context, solution: null });
    return hole;
  }
  
  solve(id, type) {
    const entry = this.holes.get(id);
    if (!entry) throw new Error(`No hole ?${id}`);
    entry.solution = type;
    entry.hole.solution = type;
  }
  
  isSolved(id) {
    const entry = this.holes.get(id);
    return entry && entry.solution !== null;
  }
  
  unsolved() {
    return [...this.holes.entries()].filter(([_, e]) => !e.solution).map(([id, e]) => ({
      id,
      context: e.context,
      suggestion: `Fill ?${id}` + (e.context ? `: expected ${e.context}` : '')
    }));
  }
  
  allSolved() { return this.unsolved().length === 0; }
  
  fillAll(solutions) {
    for (const [id, type] of Object.entries(solutions)) this.solve(Number(id), type);
  }
}

// Infer with holes
function inferWithHoles(expr, env, holeCtx) {
  switch (expr.tag) {
    case 'ENum': return new TCon('Int');
    case 'EBool': return new TCon('Bool');
    case 'EVar': return env.get(expr.name) || holeCtx.createHole(`type of ${expr.name}`);
    case 'EHole': return holeCtx.createHole(expr.hint);
    case 'ELam': {
      const paramHole = holeCtx.createHole(`param of ${expr.var}`);
      const newEnv = new Map([...env, [expr.var, paramHole]]);
      const bodyType = inferWithHoles(expr.body, newEnv, holeCtx);
      return new TFun(paramHole, bodyType);
    }
    case 'EApp': {
      const fnType = inferWithHoles(expr.fn, env, holeCtx);
      return holeCtx.createHole('return type');
    }
    default: return holeCtx.createHole();
  }
}

class ENum { constructor(n) { this.tag = 'ENum'; this.n = n; } }
class EBool { constructor(b) { this.tag = 'EBool'; this.b = b; } }
class EVar { constructor(name) { this.tag = 'EVar'; this.name = name; } }
class ELam { constructor(v, body) { this.tag = 'ELam'; this.var = v; this.body = body; } }
class EApp { constructor(fn, arg) { this.tag = 'EApp'; this.fn = fn; this.arg = arg; } }
class EHole { constructor(hint = null) { this.tag = 'EHole'; this.hint = hint; } }

export { THole, TVar, TFun, TCon, HoleContext, inferWithHoles, ENum, EBool, EVar, ELam, EApp, EHole };
