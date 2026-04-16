import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

const { Solver, parseDIMACS, encodeNQueens, encodeGraphColoring } = await import('./src/solver.cjs');

function sat(numVars, clauses) {
  const s = new Solver(numVars);
  for (const c of clauses) s.addClause(c);
  return s.solve() === 'SAT' ? s.getModel() : null;
}

describe('SAT: More Complex Formulas', () => {
  it('3-SAT random satisfiable (5 vars)', () => {
    const clauses = [
      [1, 2, 3], [-1, 4, 5], [-2, -3, 4],
      [1, -4, -5], [2, 3, -4],
    ];
    const m = sat(5, clauses);
    assert(m !== null);
  });

  it('at-most-one constraint (3 vars)', () => {
    // x1 OR x2 OR x3 (at least one)
    // NOT(x1 AND x2), NOT(x1 AND x3), NOT(x2 AND x3) (at most one)
    const clauses = [
      [1, 2, 3],
      [-1, -2], [-1, -3], [-2, -3],
    ];
    const m = sat(3, clauses);
    assert(m !== null);
    const trueCount = [1, 2, 3].filter(v => m[v]).length;
    assert.equal(trueCount, 1);
  });

  it('exactly-one (4 vars)', () => {
    const clauses = [
      [1, 2, 3, 4],
      [-1, -2], [-1, -3], [-1, -4],
      [-2, -3], [-2, -4], [-3, -4],
    ];
    const m = sat(4, clauses);
    assert(m !== null);
    const trueCount = [1, 2, 3, 4].filter(v => m[v]).length;
    assert.equal(trueCount, 1);
  });

  it('chain implications: x1→x2→x3→x4→x5, x1, ¬x5 is UNSAT', () => {
    const clauses = [
      [-1, 2], [-2, 3], [-3, 4], [-4, 5],
      [1], [-5],
    ];
    assert.equal(sat(5, clauses), null);
  });

  it('chain implications: x1→x2→x3, x1, x3 is SAT', () => {
    const clauses = [
      [-1, 2], [-2, 3], [1],
    ];
    const m = sat(3, clauses);
    assert(m !== null);
    assert.equal(m[1], true);
    assert.equal(m[2], true);
    assert.equal(m[3], true);
  });
});

describe('SAT: Systematic 2-SAT', () => {
  it('2-SAT satisfiable: (x∨y)∧(¬x∨z)∧(¬y∨¬z)', () => {
    const m = sat(3, [[1, 2], [-1, 3], [-2, -3]]);
    assert(m !== null);
  });

  it('2-SAT unsatisfiable: requires x=T,F simultaneously', () => {
    const m = sat(2, [[1, 2], [-1, -2], [1, -2], [-1, 2]]);
    assert.equal(m, null);
  });

  it('Horn clause: all implications chain SAT', () => {
    const m = sat(4, [[-1, 2], [-2, 3], [-3, 4], [1]]);
    assert(m !== null);
    assert.equal(m[4], true);
  });
});

describe('SAT: N-Queens', () => {
  it('5-queens', () => {
    const { numVars, clauses } = encodeNQueens(5);
    const m = sat(numVars, clauses);
    assert(m !== null);
  });

  it('6-queens', () => {
    const { numVars, clauses } = encodeNQueens(6);
    const m = sat(numVars, clauses);
    assert(m !== null);
  });
});

describe('SAT: Graph Coloring', () => {
  it('4-color square is satisfiable', () => {
    const { numVars, clauses } = encodeGraphColoring(
      4, // vertices
      [[0,1], [1,2], [2,3], [3,0]], // edges (square)
      4  // colors
    );
    const m = sat(numVars, clauses);
    assert(m !== null);
  });

  it('2-color bipartite graph (path) is satisfiable', () => {
    const { numVars, clauses } = encodeGraphColoring(
      3, [[0,1], [1,2]], 2
    );
    const m = sat(numVars, clauses);
    assert(m !== null);
  });

  it('2-color odd cycle (triangle) is unsatisfiable', () => {
    const { numVars, clauses } = encodeGraphColoring(
      3, [[0,1], [1,2], [2,0]], 2
    );
    assert.equal(sat(numVars, clauses), null);
  });
});

describe('SAT: Verification', () => {
  it('solution satisfies all clauses', () => {
    const clauses = [
      [1, 2, 3], [-1, 4], [-2, 5],
      [-3, -4, -5], [1, -2, 3],
      [4, 5], [-1, -5, 3],
    ];
    const m = sat(5, clauses);
    assert(m !== null);
    for (const clause of clauses) {
      const satisfied = clause.some(lit => lit > 0 ? m[Math.abs(lit)] : !m[Math.abs(lit)]);
      assert(satisfied, `Clause [${clause}] not satisfied`);
    }
  });

  it('multiple solutions possible', () => {
    // x OR y has 3 solutions
    const m = sat(2, [[1, 2]]);
    assert(m !== null);
    assert(m[1] === true || m[2] === true);
  });
});
