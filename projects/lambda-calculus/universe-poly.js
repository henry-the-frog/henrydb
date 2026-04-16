/**
 * Universe Polymorphism: Stratified type universes
 * 
 * Type : Type is inconsistent (Girard's paradox).
 * Solution: Type₀ : Type₁ : Type₂ : ...
 * Universe polymorphism: quantify over universe levels.
 */

class Level {
  constructor(n) { this.n = n; }
  succ() { return new Level(this.n + 1); }
  max(other) { return new Level(Math.max(this.n, other.n)); }
  leq(other) { return this.n <= other.n; }
  toString() { return `${this.n}`; }
}

class Universe {
  constructor(level) { this.tag = 'Universe'; this.level = level; }
  toString() { return `Type${this.level}`; }
}

// Universe of a universe
function universeOf(u) { return new Universe(u.level.succ()); }

// Pi type universe: (A : Type_i) → (B : Type_j) lives in Type_max(i,j)
function piUniverse(domLevel, codLevel) { return new Universe(domLevel.max(codLevel)); }

// Cumulativity: Type_i <: Type_j when i ≤ j
function cumulativeSubtype(u1, u2) { return u1.level.leq(u2.level); }

// Check consistency: no Type : Type
function checkConsistency(assignments) {
  for (const [term, type] of assignments) {
    if (term.tag === 'Universe' && type.tag === 'Universe') {
      if (!term.level.succ().leq(type.level)) {
        return { consistent: false, reason: `Type${term.level} : Type${type.level} but need Type${term.level.succ()}` };
      }
    }
  }
  return { consistent: true };
}

// Universe constraints
class ConstraintSet {
  constructor() { this.constraints = []; }
  addLeq(l1, l2) { this.constraints.push({ kind: 'leq', l1, l2 }); }
  addEq(l1, l2) { this.constraints.push({ kind: 'eq', l1, l2 }); }
  
  solve() {
    for (const c of this.constraints) {
      if (c.kind === 'leq' && !c.l1.leq(c.l2)) return { solved: false, constraint: c };
      if (c.kind === 'eq' && c.l1.n !== c.l2.n) return { solved: false, constraint: c };
    }
    return { solved: true };
  }
}

export { Level, Universe, universeOf, piUniverse, cumulativeSubtype, checkConsistency, ConstraintSet };
