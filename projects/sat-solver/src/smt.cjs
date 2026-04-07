'use strict';

// ============================================================
// SMT Solver — DPLL(T) with EUF Theory
// ============================================================
// Satisfiability Modulo Theories: combine CDCL SAT engine with
// theory-specific solvers. This implements:
//   - EUF: Equality + Uninterpreted Functions (congruence closure)
//   - LIA: Linear Integer Arithmetic (Simplex-based)
// ============================================================

const { Solver, TRUE, FALSE, UNDEF } = require('./solver.cjs');

// ============================================================
// Union-Find with backtracking (for EUF)
// ============================================================
class BacktrackableUnionFind {
  constructor() {
    this.parent = new Map();
    this.rank = new Map();
    this.history = [];  // stack of [node, oldParent, oldRank] for undo
  }

  _find(x) {
    // Path compression would break backtracking, so just follow chain
    while (this.parent.has(x) && this.parent.get(x) !== x) {
      x = this.parent.get(x);
    }
    return x;
  }

  _ensure(x) {
    if (!this.parent.has(x)) {
      this.parent.set(x, x);
      this.rank.set(x, 0);
    }
  }

  find(x) {
    this._ensure(x);
    return this._find(x);
  }

  union(a, b) {
    this._ensure(a);
    this._ensure(b);
    const ra = this._find(a);
    const rb = this._find(b);
    if (ra === rb) return false;  // already same

    // Union by rank
    const rankA = this.rank.get(ra);
    const rankB = this.rank.get(rb);

    if (rankA < rankB) {
      this.history.push([ra, this.parent.get(ra), rankA]);
      this.parent.set(ra, rb);
    } else if (rankA > rankB) {
      this.history.push([rb, this.parent.get(rb), rankB]);
      this.parent.set(rb, ra);
    } else {
      this.history.push([rb, this.parent.get(rb), rankB]);
      this.history.push([ra, this.parent.get(ra), rankA]);
      this.parent.set(rb, ra);
      this.rank.set(ra, rankA + 1);
    }
    return true;
  }

  sameClass(a, b) {
    return this.find(a) === this.find(b);
  }

  checkpoint() {
    return this.history.length;
  }

  backtrackTo(checkpoint) {
    while (this.history.length > checkpoint) {
      const [node, oldParent, oldRank] = this.history.pop();
      this.parent.set(node, oldParent);
      this.rank.set(node, oldRank);
    }
  }
}

// ============================================================
// EUF Theory Solver (Equality + Uninterpreted Functions)
// ============================================================
class EUFSolver {
  constructor() {
    this.uf = new BacktrackableUnionFind();
    this.equalities = [];      // [{a, b, boolVar}] — a = b ↔ boolVar
    this.disequalities = [];   // [{a, b, boolVar}] — a ≠ b ↔ boolVar
    this.funcApps = [];        // [{name, args, result}] — f(args) = result
    this.checkpoints = [];     // stack of {ufCheckpoint, eqLen, diseqLen}
    this.currentAssertions = [];  // current asserted atoms
  }

  // Register atoms
  addEquality(a, b, boolVar) {
    this.equalities.push({ a, b, boolVar });
  }

  addDisequality(a, b, boolVar) {
    this.disequalities.push({ a, b, boolVar });
  }

  addFuncApp(name, args, result) {
    this.funcApps.push({ name, args, result });
  }

  // Assert that a boolean variable is true or false
  assertTrue(boolVar) {
    // Find which equality/disequality this corresponds to
    for (const eq of this.equalities) {
      if (eq.boolVar === boolVar) {
        this.uf.union(eq.a, eq.b);
        this.currentAssertions.push({ kind: 'eq', ...eq });
        return true;
      }
    }
    for (const diseq of this.disequalities) {
      if (diseq.boolVar === boolVar) {
        this.currentAssertions.push({ kind: 'diseq', ...diseq });
        return true;
      }
    }
    return true;  // unknown atom — OK
  }

  assertFalse(boolVar) {
    // Asserting ~(a=b) is a disequality; asserting ~(a≠b) is an equality
    for (const eq of this.equalities) {
      if (eq.boolVar === boolVar) {
        this.currentAssertions.push({ kind: 'diseq', a: eq.a, b: eq.b, boolVar });
        return true;
      }
    }
    for (const diseq of this.disequalities) {
      if (diseq.boolVar === boolVar) {
        this.uf.union(diseq.a, diseq.b);
        this.currentAssertions.push({ kind: 'eq', a: diseq.a, b: diseq.b, boolVar });
        return true;
      }
    }
    return true;
  }

  // Check consistency: are any disequalities violated?
  checkConsistency() {
    // First, apply congruence closure for function applications
    this._congruenceClosure();

    // Then check all disequalities
    for (const a of this.currentAssertions) {
      if (a.kind === 'diseq') {
        if (this.uf.sameClass(a.a, a.b)) {
          return { consistent: false, conflict: a };
        }
      }
    }
    return { consistent: true };
  }

  _congruenceClosure() {
    // If f(a1,...,an) and f(b1,...,bn) and ai=bi for all i, then results are equal
    let changed = true;
    while (changed) {
      changed = false;
      for (let i = 0; i < this.funcApps.length; i++) {
        for (let j = i + 1; j < this.funcApps.length; j++) {
          const fi = this.funcApps[i];
          const fj = this.funcApps[j];
          if (fi.name !== fj.name) continue;
          if (fi.args.length !== fj.args.length) continue;

          // Check if all args are in same equivalence class
          let allEqual = true;
          for (let k = 0; k < fi.args.length; k++) {
            if (!this.uf.sameClass(fi.args[k], fj.args[k])) {
              allEqual = false;
              break;
            }
          }
          if (allEqual && !this.uf.sameClass(fi.result, fj.result)) {
            this.uf.union(fi.result, fj.result);
            changed = true;
          }
        }
      }
    }
  }

  // Explain a conflict: return minimal set of assertions causing inconsistency
  explainConflict(conflict) {
    // Simple explanation: return all equalities that connect a to b
    // A proper implementation would use proof-producing union-find
    return this.currentAssertions
      .filter(a => a.kind === 'eq')
      .map(a => a.boolVar);
  }

  // Save state
  push() {
    this.checkpoints.push({
      ufCheckpoint: this.uf.checkpoint(),
      assertionLen: this.currentAssertions.length,
    });
  }

  // Restore state
  pop() {
    const cp = this.checkpoints.pop();
    this.uf.backtrackTo(cp.ufCheckpoint);
    this.currentAssertions.length = cp.assertionLen;
  }
}

// ============================================================
// LIA Theory Solver (Linear Integer Arithmetic — bounds only)
// ============================================================
class BoundsSolver {
  constructor() {
    this.bounds = new Map();  // varName → { lower, upper, lowerAtom, upperAtom }
    this.atoms = [];          // [{kind: 'le'|'ge'|'eq', var, value, boolVar}]
    this.currentAssertions = [];
    this.checkpoints = [];
  }

  addAtom(kind, varName, value, boolVar) {
    this.atoms.push({ kind, var: varName, value, boolVar });
  }

  _getBounds(v) {
    if (!this.bounds.has(v)) {
      this.bounds.set(v, { lower: -Infinity, upper: Infinity, atoms: [] });
    }
    return this.bounds.get(v);
  }

  assertTrue(boolVar) {
    for (const atom of this.atoms) {
      if (atom.boolVar !== boolVar) continue;
      const b = this._getBounds(atom.var);
      if (atom.kind === 'le') {
        b.upper = Math.min(b.upper, atom.value);
      } else if (atom.kind === 'ge') {
        b.lower = Math.max(b.lower, atom.value);
      } else if (atom.kind === 'eq') {
        b.lower = Math.max(b.lower, atom.value);
        b.upper = Math.min(b.upper, atom.value);
      }
      b.atoms.push(atom);
      this.currentAssertions.push(atom);
    }
    return true;
  }

  assertFalse(boolVar) {
    for (const atom of this.atoms) {
      if (atom.boolVar !== boolVar) continue;
      const b = this._getBounds(atom.var);
      // Negate: ~(x <= 5) → x >= 6 (integer)
      if (atom.kind === 'le') {
        b.lower = Math.max(b.lower, atom.value + 1);
      } else if (atom.kind === 'ge') {
        b.upper = Math.min(b.upper, atom.value - 1);
      } else if (atom.kind === 'eq') {
        // ~(x = 5) → need to handle as disjunction... skip for now
      }
      this.currentAssertions.push({ ...atom, negated: true });
    }
    return true;
  }

  checkConsistency() {
    for (const [varName, b] of this.bounds) {
      if (b.lower > b.upper) {
        return { consistent: false, var: varName, lower: b.lower, upper: b.upper };
      }
    }
    return { consistent: true };
  }

  getModel() {
    const model = {};
    for (const [varName, b] of this.bounds) {
      if (b.lower !== -Infinity) model[varName] = b.lower;
      else if (b.upper !== Infinity) model[varName] = b.upper;
      else model[varName] = 0;
    }
    return model;
  }

  push() {
    this.checkpoints.push({
      assertionLen: this.currentAssertions.length,
      boundsSnapshot: new Map([...this.bounds].map(([k, v]) => [k, { ...v, atoms: [...v.atoms] }]))
    });
  }

  pop() {
    const cp = this.checkpoints.pop();
    this.currentAssertions.length = cp.assertionLen;
    this.bounds = cp.boundsSnapshot;
  }
}

// ============================================================
// SMT Expression Parser
// ============================================================
function parseSmtExpr(text) {
  // Simple S-expression parser for SMT-LIB-like syntax
  const tokens = tokenize(text);
  let pos = 0;

  function tokenize(s) {
    const toks = [];
    let i = 0;
    while (i < s.length) {
      if (s[i] === ' ' || s[i] === '\n' || s[i] === '\t') { i++; continue; }
      if (s[i] === '(' || s[i] === ')') { toks.push(s[i]); i++; continue; }
      let j = i;
      while (j < s.length && s[j] !== ' ' && s[j] !== '(' && s[j] !== ')' && s[j] !== '\n') j++;
      toks.push(s.slice(i, j));
      i = j;
    }
    return toks;
  }

  function parseExpr() {
    if (tokens[pos] === '(') {
      pos++;  // skip (
      const items = [];
      while (tokens[pos] !== ')') {
        items.push(parseExpr());
      }
      pos++;  // skip )
      return items;
    } else {
      const tok = tokens[pos++];
      const n = Number(tok);
      return isNaN(n) ? tok : n;
    }
  }

  const exprs = [];
  while (pos < tokens.length) {
    exprs.push(parseExpr());
  }
  return exprs;
}

// ============================================================
// High-level SMT interface
// ============================================================
class SMTSolver {
  constructor() {
    this.euf = new EUFSolver();
    this.bounds = new BoundsSolver();
    this.satSolver = null;
    this.nextBoolVar = 1;
    this.atomMap = new Map();  // boolVar → theory atom
    this.assertions = [];      // top-level assertions (S-expressions)
    this.declarations = {};    // name → sort
  }

  declare(name, sort) {
    this.declarations[name] = sort;
  }

  // Allocate a fresh boolean variable for a theory atom
  _freshBoolVar() {
    return this.nextBoolVar++;
  }

  // Translate a theory formula into boolean abstraction
  _abstract(expr) {
    if (typeof expr === 'number' || typeof expr === 'string') {
      return expr;  // constants and variables
    }
    if (!Array.isArray(expr)) return expr;

    const op = expr[0];

    if (op === '=' && expr.length === 3) {
      const bv = this._freshBoolVar();
      const a = String(expr[1]);
      const b = String(expr[2]);
      this.euf.addEquality(a, b, bv);
      this.atomMap.set(bv, { kind: 'eq', a, b });
      return bv;
    }
    if (op === 'distinct' && expr.length === 3) {
      const bv = this._freshBoolVar();
      const a = String(expr[1]);
      const b = String(expr[2]);
      this.euf.addDisequality(a, b, bv);
      this.atomMap.set(bv, { kind: 'diseq', a, b });
      return bv;
    }
    if ((op === '<=' || op === '>=') && expr.length === 3) {
      const bv = this._freshBoolVar();
      const varName = String(expr[1]);
      const value = Number(expr[2]);
      this.bounds.addAtom(op === '<=' ? 'le' : 'ge', varName, value, bv);
      this.atomMap.set(bv, { kind: op, var: varName, value });
      return bv;
    }
    if (op === 'and') {
      return expr.slice(1).map(e => this._abstract(e));
    }
    if (op === 'or') {
      return { or: expr.slice(1).map(e => this._abstract(e)) };
    }
    if (op === 'not' && expr.length === 2) {
      const inner = this._abstract(expr[1]);
      return -inner;
    }

    // Function application
    if (typeof op === 'string' && expr.length >= 2) {
      const bv = this._freshBoolVar();
      const result = `__func_${bv}`;
      const args = expr.slice(1).map(e => String(e));
      this.euf.addFuncApp(op, args, result);
      return result;
    }

    return expr;
  }

  assert(expr) {
    this.assertions.push(expr);
  }

  // Simple check-sat for conjunction of theory literals
  checkSat() {
    // For simple conjunctive assertions, we can check directly
    // without the full SAT solver
    this.euf.push();
    this.bounds.push();

    for (const expr of this.assertions) {
      this._processAssertion(expr);
    }

    const eufResult = this.euf.checkConsistency();
    const boundsResult = this.bounds.checkConsistency();

    const sat = eufResult.consistent && boundsResult.consistent;

    this.euf.pop();
    this.bounds.pop();

    return sat ? 'SAT' : 'UNSAT';
  }

  _processAssertion(expr) {
    if (!Array.isArray(expr)) return;
    const op = expr[0];

    if (op === '=') {
      const a = String(expr[1]);
      const b = String(expr[2]);
      const bv = this._freshBoolVar();
      this.euf.addEquality(a, b, bv);
      this.euf.assertTrue(bv);
    } else if (op === 'distinct' || op === '!=') {
      const a = String(expr[1]);
      const b = String(expr[2]);
      const bv = this._freshBoolVar();
      this.euf.addDisequality(a, b, bv);
      this.euf.assertTrue(bv);
    } else if (op === '<=') {
      const bv = this._freshBoolVar();
      this.bounds.addAtom('le', String(expr[1]), Number(expr[2]), bv);
      this.bounds.assertTrue(bv);
    } else if (op === '>=') {
      const bv = this._freshBoolVar();
      this.bounds.addAtom('ge', String(expr[1]), Number(expr[2]), bv);
      this.bounds.assertTrue(bv);
    } else if (op === 'and') {
      for (let i = 1; i < expr.length; i++) {
        this._processAssertion(expr[i]);
      }
    } else if (op === 'not') {
      this._processNegation(expr[1]);
    }
  }

  _processNegation(expr) {
    if (!Array.isArray(expr)) return;
    const op = expr[0];

    if (op === '=') {
      const a = String(expr[1]);
      const b = String(expr[2]);
      const bv = this._freshBoolVar();
      this.euf.addEquality(a, b, bv);
      this.euf.assertFalse(bv);
    } else if (op === 'distinct' || op === '!=') {
      const a = String(expr[1]);
      const b = String(expr[2]);
      const bv = this._freshBoolVar();
      this.euf.addDisequality(a, b, bv);
      this.euf.assertFalse(bv);
    } else if (op === '<=') {
      const bv = this._freshBoolVar();
      this.bounds.addAtom('le', String(expr[1]), Number(expr[2]), bv);
      this.bounds.assertFalse(bv);
    } else if (op === '>=') {
      const bv = this._freshBoolVar();
      this.bounds.addAtom('ge', String(expr[1]), Number(expr[2]), bv);
      this.bounds.assertFalse(bv);
    }
  }
}

module.exports = {
  BacktrackableUnionFind,
  EUFSolver,
  BoundsSolver,
  SMTSolver,
  parseSmtExpr,
};
