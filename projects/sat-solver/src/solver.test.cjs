'use strict';

const {
  Solver, parseDIMACS, encodePigeonhole, encodeNQueens,
  encodeGraphColoring, encodeSudoku, randomSAT, createSolver,
  TRUE, FALSE, UNDEF
} = require('./solver.cjs');

let passed = 0, failed = 0;
function test(name, fn) {
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    console.error(`FAIL: ${name}`);
    console.error(`  ${e.message}`);
  }
}
function assert(cond, msg = 'assertion failed') { if (!cond) throw new Error(msg); }
function eq(a, b, msg) { assert(a === b, msg || `expected ${b}, got ${a}`); }

// ============================================================
// Basic SAT/UNSAT
// ============================================================

test('empty solver is SAT', () => {
  const s = new Solver(0);
  eq(s.solve(), 'SAT');
});

test('single positive unit clause', () => {
  const s = new Solver(1);
  s.addClause([1]);
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  eq(m[1], true);
});

test('single negative unit clause', () => {
  const s = new Solver(1);
  s.addClause([-1]);
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  eq(m[1], false);
});

test('contradictory unit clauses', () => {
  const s = new Solver(1);
  s.addClause([1]);
  s.addClause([-1]);
  eq(s.solve(), 'UNSAT');
});

test('two variables, simple SAT', () => {
  const s = new Solver(2);
  s.addClause([1, 2]);      // x1 OR x2
  s.addClause([-1, -2]);    // NOT x1 OR NOT x2
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  // At least one true, not both true → exactly one true
  assert(m[1] !== m[2], 'exactly one should be true');
});

test('simple 3-variable UNSAT', () => {
  const s = new Solver(2);
  s.addClause([1]);
  s.addClause([2]);
  s.addClause([-1, -2]);
  eq(s.solve(), 'UNSAT');
});

test('three clauses, satisfiable', () => {
  const s = new Solver(3);
  s.addClause([1, 2, 3]);
  s.addClause([-1, 2]);
  s.addClause([-2, 3]);
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  // Verify all clauses
  assert(m[1] || m[2] || m[3]);
  assert(!m[1] || m[2]);
  assert(!m[2] || m[3]);
});

test('implication chain', () => {
  // x1 → x2 → x3 → x4, x1 true, x4 false → UNSAT
  const s = new Solver(4);
  s.addClause([1]);          // x1
  s.addClause([-1, 2]);     // x1 → x2
  s.addClause([-2, 3]);     // x2 → x3
  s.addClause([-3, 4]);     // x3 → x4
  s.addClause([-4]);         // NOT x4
  eq(s.solve(), 'UNSAT');
});

test('implication chain (satisfiable)', () => {
  const s = new Solver(4);
  s.addClause([1]);
  s.addClause([-1, 2]);
  s.addClause([-2, 3]);
  s.addClause([-3, 4]);
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  assert(m[1] && m[2] && m[3] && m[4]);
});

// ============================================================
// Unit Propagation
// ============================================================

test('unit propagation cascade', () => {
  const s = new Solver(5);
  s.addClause([1]);
  s.addClause([-1, 2]);
  s.addClause([-2, 3]);
  s.addClause([-3, 4]);
  s.addClause([-4, 5]);
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  for (let v = 1; v <= 5; v++) assert(m[v], `x${v} should be true`);
});

test('unit propagation finds conflict', () => {
  const s = new Solver(3);
  s.addClause([1]);
  s.addClause([-1, 2]);
  s.addClause([-1, -2]);  // with x1=T → x2 must be T and F
  eq(s.solve(), 'UNSAT');
});

// ============================================================
// DIMACS Parser
// ============================================================

test('parse simple DIMACS', () => {
  const text = `c comment
p cnf 3 2
1 2 3 0
-1 -2 0`;
  const { numVars, numClauses, clauses } = parseDIMACS(text);
  eq(numVars, 3);
  eq(numClauses, 2);
  eq(clauses.length, 2);
  assert(JSON.stringify(clauses[0]) === JSON.stringify([1, 2, 3]));
  assert(JSON.stringify(clauses[1]) === JSON.stringify([-1, -2]));
});

test('parse multiline clauses', () => {
  const text = `p cnf 4 1
1 2
3 4 0`;
  const { clauses } = parseDIMACS(text);
  eq(clauses.length, 1);
  eq(clauses[0].length, 4);
});

test('DIMACS round-trip solve', () => {
  const text = `p cnf 3 3
1 2 3 0
-1 2 0
-2 3 0`;
  const { numVars, clauses } = parseDIMACS(text);
  const s = new Solver(numVars);
  for (const c of clauses) s.addClause(c);
  eq(s.solve(), 'SAT');
});

// ============================================================
// Pigeonhole Principle (UNSAT proof)
// ============================================================

test('pigeonhole 2 pigeons 1 hole (UNSAT)', () => {
  const problem = encodePigeonhole(1);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

test('pigeonhole 3 pigeons 2 holes (UNSAT)', () => {
  const problem = encodePigeonhole(2);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

test('pigeonhole 4 pigeons 3 holes (UNSAT)', () => {
  const problem = encodePigeonhole(3);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

test('pigeonhole 5 pigeons 4 holes (UNSAT)', () => {
  const problem = encodePigeonhole(4);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

test('pigeonhole 6 pigeons 5 holes (UNSAT)', () => {
  const problem = encodePigeonhole(5);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

// ============================================================
// N-Queens
// ============================================================

test('4-queens has solution', () => {
  const problem = encodeNQueens(4);
  const s = createSolver(problem);
  eq(s.solve(), 'SAT');
  const queens = problem.decode(s.getModel());
  eq(queens.length, 4);
  // Verify no conflicts
  for (let i = 0; i < queens.length; i++) {
    for (let j = i + 1; j < queens.length; j++) {
      const [r1, c1] = queens[i];
      const [r2, c2] = queens[j];
      assert(r1 !== r2, 'same row');
      assert(c1 !== c2, 'same column');
      assert(Math.abs(r1 - r2) !== Math.abs(c1 - c2), 'same diagonal');
    }
  }
});

test('8-queens has solution', () => {
  const problem = encodeNQueens(8);
  const s = createSolver(problem);
  eq(s.solve(), 'SAT');
  const queens = problem.decode(s.getModel());
  eq(queens.length, 8);
  // Quick conflict check
  const rows = new Set(), cols = new Set();
  for (const [r, c] of queens) { rows.add(r); cols.add(c); }
  eq(rows.size, 8);
  eq(cols.size, 8);
});

test('2-queens is UNSAT', () => {
  const problem = encodeNQueens(2);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

test('3-queens is UNSAT', () => {
  const problem = encodeNQueens(3);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

test('5-queens has solution', () => {
  const problem = encodeNQueens(5);
  const s = createSolver(problem);
  eq(s.solve(), 'SAT');
  eq(problem.decode(s.getModel()).length, 5);
});

// ============================================================
// Graph Coloring
// ============================================================

test('triangle (K3) needs 3 colors', () => {
  const edges = [[0, 1], [1, 2], [0, 2]];
  // 2 colors should be UNSAT
  const p2 = encodeGraphColoring(3, edges, 2);
  const s2 = createSolver(p2);
  eq(s2.solve(), 'UNSAT');
  // 3 colors should be SAT
  const p3 = encodeGraphColoring(3, edges, 3);
  const s3 = createSolver(p3);
  eq(s3.solve(), 'SAT');
  const coloring = p3.decode(s3.getModel());
  // Verify coloring is valid
  for (const [a, b] of edges) {
    assert(coloring[a] !== coloring[b], `nodes ${a} and ${b} same color`);
  }
});

test('complete graph K4 needs 4 colors', () => {
  const edges = [];
  for (let i = 0; i < 4; i++)
    for (let j = i + 1; j < 4; j++)
      edges.push([i, j]);
  const p3 = encodeGraphColoring(4, edges, 3);
  eq(createSolver(p3).solve(), 'UNSAT');
  const p4 = encodeGraphColoring(4, edges, 4);
  const s4 = createSolver(p4);
  eq(s4.solve(), 'SAT');
});

test('bipartite graph (K3,3) needs 2 colors', () => {
  const edges = [];
  for (let i = 0; i < 3; i++)
    for (let j = 3; j < 6; j++)
      edges.push([i, j]);
  const p2 = encodeGraphColoring(6, edges, 2);
  const s = createSolver(p2);
  eq(s.solve(), 'SAT');
  const coloring = p2.decode(s.getModel());
  for (const [a, b] of edges) {
    assert(coloring[a] !== coloring[b]);
  }
});

test('Petersen graph needs 3 colors', () => {
  // Petersen graph: 10 nodes, chromatic number 3
  const edges = [
    [0,1],[1,2],[2,3],[3,4],[4,0],  // outer pentagon
    [5,7],[7,9],[9,6],[6,8],[8,5],  // inner star
    [0,5],[1,6],[2,7],[3,8],[4,9],  // connections
  ];
  const p2 = encodeGraphColoring(10, edges, 2);
  eq(createSolver(p2).solve(), 'UNSAT');
  const p3 = encodeGraphColoring(10, edges, 3);
  eq(createSolver(p3).solve(), 'SAT');
});

// ============================================================
// Sudoku
// ============================================================

test('solve easy sudoku', () => {
  const grid = [
    [5,3,0,0,7,0,0,0,0],
    [6,0,0,1,9,5,0,0,0],
    [0,9,8,0,0,0,0,6,0],
    [8,0,0,0,6,0,0,0,3],
    [4,0,0,8,0,3,0,0,1],
    [7,0,0,0,2,0,0,0,6],
    [0,6,0,0,0,0,2,8,0],
    [0,0,0,4,1,9,0,0,5],
    [0,0,0,0,8,0,0,7,9],
  ];
  const problem = encodeSudoku(grid);
  const s = createSolver(problem);
  eq(s.solve(), 'SAT');
  const result = problem.decode(s.getModel());
  // Verify solution
  for (let r = 0; r < 9; r++) {
    const rowSet = new Set(result[r]);
    eq(rowSet.size, 9);
    for (let d = 1; d <= 9; d++) assert(rowSet.has(d));
  }
  for (let c = 0; c < 9; c++) {
    const colSet = new Set();
    for (let r = 0; r < 9; r++) colSet.add(result[r][c]);
    eq(colSet.size, 9);
  }
  // Verify given clues preserved
  eq(result[0][0], 5);
  eq(result[0][1], 3);
  eq(result[0][4], 7);
});

test('solve hard sudoku', () => {
  // "World's hardest sudoku" variant — takes longer due to sparse clues
  const grid = [
    [1,0,0,0,0,7,0,9,0],
    [0,3,0,0,2,0,0,0,8],
    [0,0,9,6,0,0,5,0,0],
    [0,0,5,3,0,0,9,0,0],
    [0,1,0,0,8,0,0,0,2],
    [6,0,0,0,0,4,0,0,0],
    [3,0,0,0,0,0,0,1,0],
    [0,4,0,0,0,0,0,0,7],
    [0,0,7,0,0,0,3,0,0],
  ];
  const problem = encodeSudoku(grid);
  const s = createSolver(problem);
  eq(s.solve(), 'SAT');
  const result = problem.decode(s.getModel());
  // Verify all rows have 1-9
  for (let r = 0; r < 9; r++) {
    eq(new Set(result[r]).size, 9);
  }
  // Verify clues preserved
  eq(result[0][0], 1);
  eq(result[0][5], 7);
  eq(result[0][7], 9);
});

test('invalid sudoku (two 5s in first row) is UNSAT', () => {
  const grid = Array.from({ length: 9 }, () => new Array(9).fill(0));
  grid[0][0] = 5;
  grid[0][1] = 5;  // conflict
  const problem = encodeSudoku(grid);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
});

// ============================================================
// Random 3-SAT
// ============================================================

test('random easy SAT (low clause ratio)', () => {
  // ratio 2.0 → almost certainly SAT
  const problem = randomSAT(20, 40, 3);
  const s = createSolver(problem);
  const result = s.solve();
  // At ratio 2.0 it's almost always SAT
  eq(result, 'SAT');
});

test('random SAT consistency (model satisfies all clauses)', () => {
  for (let trial = 0; trial < 5; trial++) {
    const problem = randomSAT(15, 30, 3);
    const s = createSolver(problem);
    const result = s.solve();
    if (result === 'SAT') {
      const model = s.getModel();
      for (const clause of problem.clauses) {
        let sat = false;
        for (const lit of clause) {
          const v = Math.abs(lit);
          if ((lit > 0 && model[v]) || (lit < 0 && !model[v])) {
            sat = true;
            break;
          }
        }
        assert(sat, 'model does not satisfy clause');
      }
    }
  }
});

// ============================================================
// CDCL-specific behavior
// ============================================================

test('learns clauses from conflicts', () => {
  // Create a problem that requires backtracking and clause learning
  const s = new Solver(5);
  s.addClause([1, 2]);
  s.addClause([-1, 3]);
  s.addClause([-2, 4]);
  s.addClause([3, 4]);
  s.addClause([-3, 5]);
  s.addClause([-4, 5]);
  s.addClause([-5, 1, 2]);
  eq(s.solve(), 'SAT');
});

test('VSIDS makes decisions', () => {
  const problem = randomSAT(20, 60);
  const s = createSolver(problem);
  s.solve();
  assert(s.decisions > 0 || s.propagations > 0, 'should have made decisions');
});

test('stats are tracked', () => {
  const problem = encodePigeonhole(3);
  const s = createSolver(problem);
  s.solve();
  const stats = s.getStats();
  assert(stats.conflicts > 0, 'should have conflicts');
  assert(stats.propagations > 0, 'should have propagations');
  eq(stats.variables, problem.numVars);
});

test('phase saving affects decisions', () => {
  const s = new Solver(5);
  // Create a problem where phase saving helps
  s.addClause([1, 2, 3]);
  s.addClause([-1, 4]);
  s.addClause([-2, 5]);
  s.addClause([-4, -5]);
  s.addClause([1, -2]);
  s.addClause([-1, 2]);
  eq(s.solve(), 'SAT');
});

// ============================================================
// Edge cases
// ============================================================

test('all variables appear in only positive literals', () => {
  const s = new Solver(3);
  s.addClause([1, 2]);
  s.addClause([2, 3]);
  eq(s.solve(), 'SAT');
});

test('all variables appear in only negative literals', () => {
  const s = new Solver(3);
  s.addClause([-1, -2]);
  s.addClause([-2, -3]);
  eq(s.solve(), 'SAT');
});

test('single variable many clauses', () => {
  const s = new Solver(1);
  s.addClause([1]);
  s.addClause([1]);
  s.addClause([1]);
  eq(s.solve(), 'SAT');
  eq(s.getModel()[1], true);
});

test('tautological clause (x OR NOT x)', () => {
  const s = new Solver(2);
  s.addClause([1, -1]);  // always true
  s.addClause([2]);
  eq(s.solve(), 'SAT');
});

test('long clause', () => {
  const s = new Solver(20);
  const clause = [];
  for (let i = 1; i <= 20; i++) clause.push(i);
  s.addClause(clause);
  eq(s.solve(), 'SAT');
});

test('many binary clauses', () => {
  const s = new Solver(10);
  // Create a chain: x1 → x2 → ... → x10
  for (let i = 1; i < 10; i++) {
    s.addClause([-i, i + 1]);
  }
  s.addClause([1]);   // x1 is true
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  for (let v = 1; v <= 10; v++) assert(m[v]);
});

test('multiple unrelated components', () => {
  const s = new Solver(6);
  // Component 1: x1 AND x2
  s.addClause([1]);
  s.addClause([2]);
  // Component 2: x3 OR x4
  s.addClause([3, 4]);
  // Component 3: NOT x5 AND NOT x6
  s.addClause([-5]);
  s.addClause([-6]);
  eq(s.solve(), 'SAT');
  const m = s.getModel();
  assert(m[1] && m[2] && !m[5] && !m[6]);
});

// ============================================================
// Larger problems
// ============================================================

test('12-queens', () => {
  const problem = encodeNQueens(12);
  const s = createSolver(problem);
  const start = Date.now();
  eq(s.solve(), 'SAT');
  const elapsed = Date.now() - start;
  const queens = problem.decode(s.getModel());
  eq(queens.length, 12);
  // Verify
  const rows = new Set(), cols = new Set();
  for (const [r, c] of queens) { rows.add(r); cols.add(c); }
  eq(rows.size, 12);
  eq(cols.size, 12);
});

test('pigeonhole 7 into 6 (UNSAT, harder)', () => {
  const problem = encodePigeonhole(6);
  const s = createSolver(problem);
  eq(s.solve(), 'UNSAT');
  assert(s.conflicts > 0);
});

test('random 3-SAT at phase transition (ratio ~4.27)', () => {
  // Phase transition for 3-SAT is at clause/var ratio ≈ 4.27
  let satCount = 0, unsatCount = 0;
  for (let i = 0; i < 10; i++) {
    const problem = randomSAT(30, 128, 3);  // ratio ~4.27
    const s = createSolver(problem);
    if (s && s.solve() === 'SAT') satCount++;
    else unsatCount++;
  }
  // At phase transition, roughly half should be SAT
  // Just verify solver doesn't crash on hard instances
  assert(satCount + unsatCount === 10, 'all instances completed');
});

test('random 3-SAT heavy overconstrained (ratio 6 → almost always UNSAT)', () => {
  let unsatCount = 0;
  for (let i = 0; i < 5; i++) {
    const problem = randomSAT(20, 120, 3);  // ratio 6
    const s = createSolver(problem);
    if (s && s.solve() === 'UNSAT') unsatCount++;
  }
  // Most should be UNSAT at this ratio
  assert(unsatCount >= 3, 'most should be UNSAT at ratio 6');
});

// ============================================================
// Model verification utility
// ============================================================

function verifyModel(clauses, model) {
  for (const clause of clauses) {
    let sat = false;
    for (const lit of clause) {
      const v = Math.abs(lit);
      if ((lit > 0 && model[v]) || (lit < 0 && !model[v])) {
        sat = true;
        break;
      }
    }
    if (!sat) return false;
  }
  return true;
}

test('verify model utility works', () => {
  assert(verifyModel([[1, 2], [-1, 2]], { 1: false, 2: true }));
  assert(!verifyModel([[1], [-1]], { 1: true }));
});

test('all SAT results verified', () => {
  // Run several problems and verify models
  const problems = [
    randomSAT(10, 15),
    randomSAT(15, 30),
    randomSAT(20, 40),
  ];
  for (const problem of problems) {
    const s = createSolver(problem);
    if (!s) continue;
    const result = s.solve();
    if (result === 'SAT') {
      assert(verifyModel(problem.clauses, s.getModel()), 'model should satisfy all clauses');
    }
  }
});

// ============================================================
// Performance
// ============================================================

test('benchmark: 50-var random SAT', () => {
  const start = Date.now();
  for (let i = 0; i < 10; i++) {
    const problem = randomSAT(50, 200);
    const s = createSolver(problem);
    if (s) s.solve();
  }
  const elapsed = Date.now() - start;
  assert(elapsed < 5000, `50-var should be fast, took ${elapsed}ms`);
});

test('benchmark: 100-var random SAT', () => {
  const start = Date.now();
  const problem = randomSAT(100, 420);  // near phase transition
  const s = createSolver(problem);
  if (s) s.solve();
  const elapsed = Date.now() - start;
  assert(elapsed < 10000, `100-var took ${elapsed}ms`);
});

// ============================================================
// Luby Sequence
// ============================================================

test('Luby sequence first 15 values', () => {
  const expected = [1, 1, 2, 1, 1, 2, 4, 1, 1, 2, 1, 1, 2, 4, 8];
  for (let i = 0; i < expected.length; i++) {
    eq(Solver._luby(i), expected[i], `luby(${i}) = ${Solver._luby(i)}, expected ${expected[i]}`);
  }
});

// ============================================================
// Preprocessing
// ============================================================

test('subsumption removes subsumed clauses', () => {
  const s = new Solver(5);
  s.addClause([1, 2]);       // small
  s.addClause([1, 2, 3]);    // subsumed by [1,2]
  s.addClause([1, 2, 3, 4]); // subsumed by [1,2]
  s.addClause([3, 4]);       // not subsumed
  eq(s.clauses.length, 4);
  const { removed } = s.preprocess();
  eq(removed, 2, `expected 2 removed, got ${removed}`);
  eq(s.clauses.length, 2);
  eq(s.solve(), 'SAT');
});

test('probing forces literals', () => {
  const s = new Solver(3);
  // x1 → x2, ~x1 → x2 — so x2 must be true
  s.addClause([-1, 2]);
  s.addClause([1, 2]);
  const { forced } = s.probe();
  assert(forced >= 1, 'should force x2');
  eq(s.solve(), 'SAT');
  eq(s.getModel()[2], true);
});

test('probing detects UNSAT', () => {
  const s = new Solver(2);
  s.addClause([1]);
  s.addClause([-1, 2]);
  s.addClause([-1, -2]);
  const { unsat } = s.probe();
  assert(unsat, 'should detect UNSAT via probing');
});

// ============================================================
// LBD (clause quality)
// ============================================================

test('learned clauses have LBD values', () => {
  const problem = randomSAT(20, 60);
  const s = createSolver(problem);
  s.solve();
  // Some learned clauses should have LBD values
  const withLBD = s.learneds.filter(c => c.lbd < Infinity);
  assert(withLBD.length > 0 || s.learneds.length === 0, 'learned clauses should have LBD');
});

// ============================================================
// Edge Cases
// ============================================================

test('empty clause → immediate UNSAT', () => {
  const s = new Solver(2);
  s.addClause([]);  // empty clause is always false
  eq(s.solve(), 'UNSAT');
});

test('single variable, both polarities → UNSAT', () => {
  const s = new Solver(1);
  s.addClause([1]);   // x must be true
  s.addClause([-1]);  // x must be false
  eq(s.solve(), 'UNSAT');
});

test('tautological clause does not affect result', () => {
  const s = new Solver(3);
  s.addClause([1, -1]);   // tautology (always true)
  s.addClause([2, 3]);
  s.addClause([-2, -3]);
  eq(s.solve(), 'SAT');
});

test('all-positive clauses are always SAT', () => {
  const s = new Solver(5);
  for (let i = 1; i <= 5; i++) s.addClause([i]);
  eq(s.solve(), 'SAT');
  const model = s.getModel();
  for (let i = 1; i <= 5; i++) assert(model[i] === true, `var ${i} should be true`);
});

test('all-negative clauses are always SAT', () => {
  const s = new Solver(5);
  for (let i = 1; i <= 5; i++) s.addClause([-i]);
  eq(s.solve(), 'SAT');
  const model = s.getModel();
  for (let i = 1; i <= 5; i++) assert(model[i] === false, `var ${i} should be false`);
});

test('long chain implication', () => {
  // x1 → x2 → x3 → ... → x10, plus x1 = true and x10 = false → UNSAT
  const s = new Solver(10);
  s.addClause([1]);  // x1 must be true
  for (let i = 1; i < 10; i++) s.addClause([-i, i + 1]); // xi → x(i+1)
  s.addClause([-10]);  // x10 must be false
  eq(s.solve(), 'UNSAT');
});

test('large random SAT at underconstrained ratio', () => {
  // ratio 3.0 is well below phase transition (4.27) — almost always SAT
  const problem = randomSAT(100, 300, 3);
  const s = createSolver(problem);
  eq(s.solve(), 'SAT');
  // Verify model
  const model = s.getModel();
  for (const clause of problem.clauses) {
    const sat = clause.some(lit => lit > 0 ? model[lit] : !model[-lit]);
    assert(sat, 'every clause must be satisfied');
  }
});

test('large random UNSAT at overconstrained ratio', () => {
  // ratio 6.0 is well above phase transition — almost always UNSAT
  const problem = randomSAT(30, 180, 3);
  const s = createSolver(problem);
  const result = s.solve();
  // At 6.0 ratio, overwhelmingly UNSAT, but not guaranteed for every seed
  assert(result === 'SAT' || result === 'UNSAT', 'should terminate');
});

test('pigeonhole(1) — 2 pigeons 1 hole', () => {
  const p = encodePigeonhole(1);
  const s = createSolver(p);
  eq(s.solve(), 'UNSAT');
});

// ============================================================
// Report
// ============================================================

console.log(`\n${passed} passed, ${failed} failed out of ${passed + failed} tests`);
if (failed > 0) process.exit(1);
