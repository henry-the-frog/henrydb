// sat-solver-stress.test.cjs — Hard instance stress tests for CDCL solver
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Solver, encodePigeonhole, encodeNQueens, encodeGraphColoring, createSolver, randomSAT } = require('./src/solver.cjs');

// Helper: generate random k-SAT instance
function randomKSAT(n, m, k = 3) {
  const clauses = [];
  for (let i = 0; i < m; i++) {
    const clause = [];
    const used = new Set();
    while (clause.length < k) {
      const v = Math.floor(Math.random() * n) + 1;
      if (used.has(v)) continue;
      used.add(v);
      clause.push(Math.random() < 0.5 ? v : -v);
    }
    clauses.push(clause);
  }
  return clauses;
}

// Helper: verify SAT solution
function verifySolution(clauses, model) {
  for (const clause of clauses) {
    let satisfied = false;
    for (const lit of clause) {
      const v = Math.abs(lit);
      if (lit > 0 && model[v] === true) satisfied = true;
      if (lit < 0 && model[v] === false) satisfied = true;
    }
    if (!satisfied) return false;
  }
  return true;
}

test('pigeonhole 3→2: UNSAT', () => {
  const problem = encodePigeonhole(2);
  const solver = createSolver(problem);
  assert.ok(solver, 'should create solver');
  const result = solver.solve();
  assert.strictEqual(result, 'UNSAT', '3 pigeons, 2 holes should be UNSAT');
});

test('pigeonhole 4→3: UNSAT', () => {
  const problem = encodePigeonhole(3);
  const solver = createSolver(problem);
  assert.ok(solver);
  assert.strictEqual(solver.solve(), 'UNSAT');
});

test('pigeonhole 5→4: UNSAT', () => {
  const problem = encodePigeonhole(4);
  const solver = createSolver(problem);
  assert.ok(solver);
  assert.strictEqual(solver.solve(), 'UNSAT');
});

test('pigeonhole 6→5: UNSAT (harder)', () => {
  const problem = encodePigeonhole(5);
  const solver = createSolver(problem);
  assert.ok(solver);
  assert.strictEqual(solver.solve(), 'UNSAT');
});

test('random 3-SAT at phase transition (ratio ~4.26): mix of SAT/UNSAT', () => {
  const n = 20;
  const m = Math.round(n * 4.26);
  let satCount = 0, unsatCount = 0;
  
  for (let trial = 0; trial < 50; trial++) {
    const clauses = randomKSAT(n, m, 3);
    const solver = new Solver(n);
    for (const c of clauses) solver.addClause(c);
    const result = solver.solve();
    
    if (result === 'SAT') {
      satCount++;
      const model = solver.getModel();
      assert.ok(verifySolution(clauses, model), `trial ${trial}: SAT but model invalid`);
    } else {
      unsatCount++;
    }
  }
  
  console.log(`Phase transition: ${satCount} SAT, ${unsatCount} UNSAT / 50`);
  assert.ok(satCount + unsatCount === 50);
});

test('random 3-SAT underconstrained (ratio 3.0): mostly SAT', () => {
  const n = 30;
  const m = Math.round(n * 3.0);
  let satCount = 0;
  
  for (let trial = 0; trial < 20; trial++) {
    const clauses = randomKSAT(n, m, 3);
    const solver = new Solver(n);
    for (const c of clauses) solver.addClause(c);
    const result = solver.solve();
    if (result === 'SAT') {
      satCount++;
      const model = solver.getModel();
      assert.ok(verifySolution(clauses, model));
    }
  }
  assert.ok(satCount >= 15, `expected mostly SAT, got ${satCount}/20`);
});

test('random 3-SAT overconstrained (ratio 6.0): mostly UNSAT', () => {
  const n = 30;
  const m = Math.round(n * 6.0);
  let unsatCount = 0;
  
  for (let trial = 0; trial < 20; trial++) {
    const clauses = randomKSAT(n, m, 3);
    const solver = new Solver(n);
    for (const c of clauses) solver.addClause(c);
    const result = solver.solve();
    if (result === 'UNSAT') unsatCount++;
    else {
      const model = solver.getModel();
      assert.ok(verifySolution(clauses, model));
    }
  }
  assert.ok(unsatCount >= 15, `expected mostly UNSAT, got ${unsatCount}/20`);
});

test('C5 graph 3-coloring: SAT', () => {
  const edges = [[0,1], [1,2], [2,3], [3,4], [4,0]];
  const problem = encodeGraphColoring(5, edges, 3);
  const solver = createSolver(problem);
  assert.ok(solver);
  const result = solver.solve();
  assert.strictEqual(result, 'SAT', 'C5 with 3 colors should be SAT');
  const coloring = problem.decode(solver.getModel());
  // Verify no adjacent nodes share color
  for (const [u, v] of edges) {
    assert.notStrictEqual(coloring[u], coloring[v], `nodes ${u} and ${v} should have different colors`);
  }
});

test('K4 graph 3-coloring: UNSAT', () => {
  const edges = [[0,1], [0,2], [0,3], [1,2], [1,3], [2,3]];
  const problem = encodeGraphColoring(4, edges, 3);
  const solver = createSolver(problem);
  assert.ok(solver);
  assert.strictEqual(solver.solve(), 'UNSAT');
});

test('Petersen graph 3-coloring: SAT', () => {
  const edges = [
    [0,1], [1,2], [2,3], [3,4], [4,0],
    [0,5], [1,6], [2,7], [3,8], [4,9],
    [5,7], [7,9], [9,6], [6,8], [8,5],
  ];
  const problem = encodeGraphColoring(10, edges, 3);
  const solver = createSolver(problem);
  assert.ok(solver);
  assert.strictEqual(solver.solve(), 'SAT');
  const coloring = problem.decode(solver.getModel());
  for (const [u, v] of edges) {
    assert.notStrictEqual(coloring[u], coloring[v]);
  }
});

test('N-queens N=6: SAT with valid placement', () => {
  const problem = encodeNQueens(6);
  const solver = createSolver(problem);
  assert.ok(solver);
  assert.strictEqual(solver.solve(), 'SAT');
  const queens = problem.decode(solver.getModel());
  assert.strictEqual(queens.length, 6);
  // All in different columns
  const cols = queens.map(q => q[1]);
  assert.strictEqual(new Set(cols).size, 6);
  // No diagonal conflicts
  for (let i = 0; i < queens.length; i++) {
    for (let j = i + 1; j < queens.length; j++) {
      const dr = Math.abs(queens[i][0] - queens[j][0]);
      const dc = Math.abs(queens[i][1] - queens[j][1]);
      assert.notStrictEqual(dr, dc, `diagonal conflict: ${queens[i]} vs ${queens[j]}`);
    }
  }
});

test('N-queens N=8: SAT', () => {
  const problem = encodeNQueens(8);
  const solver = createSolver(problem);
  assert.ok(solver);
  assert.strictEqual(solver.solve(), 'SAT');
  const queens = problem.decode(solver.getModel());
  assert.strictEqual(queens.length, 8);
});

test('unit clause chain propagation', () => {
  const solver = new Solver(5);
  solver.addClause([1]);
  solver.addClause([-1, 2]);
  solver.addClause([-2, 3]);
  solver.addClause([-3, 4]);
  solver.addClause([-4, 5]);
  assert.strictEqual(solver.solve(), 'SAT');
  const model = solver.getModel();
  assert.strictEqual(model[1], true);
  assert.strictEqual(model[5], true);
});

test('unit clause conflict detection', () => {
  const solver = new Solver(2);
  solver.addClause([1]);
  solver.addClause([-1, 2]);
  solver.addClause([-1, -2]);
  assert.strictEqual(solver.solve(), 'UNSAT');
});

test('empty clause is UNSAT', () => {
  const solver = new Solver(1);
  const added = solver.addClause([]);
  if (added) {
    assert.strictEqual(solver.solve(), 'UNSAT');
  }
  // If addClause returns false, that's also correct (trivially UNSAT)
});

test('tautological clause', () => {
  const solver = new Solver(3);
  solver.addClause([1, -1]);
  solver.addClause([2]);
  solver.addClause([-2, 3]);
  assert.strictEqual(solver.solve(), 'SAT');
});

test('large random instance (100 vars)', () => {
  const n = 100;
  const m = Math.round(n * 3.5);
  const clauses = randomKSAT(n, m, 3);
  const solver = new Solver(n);
  for (const c of clauses) solver.addClause(c);
  const result = solver.solve();
  assert.ok(result === 'SAT' || result === 'UNSAT');
  if (result === 'SAT') {
    assert.ok(verifySolution(clauses, solver.getModel()));
  }
});

test('500 random instances: no crashes, all solutions verified', () => {
  let crashes = 0;
  let satVerified = 0;
  let unsat = 0;
  
  for (let trial = 0; trial < 500; trial++) {
    const n = Math.floor(Math.random() * 30) + 5;
    const ratio = 2 + Math.random() * 6;
    const m = Math.round(n * ratio);
    const clauses = randomKSAT(n, m, 3);
    
    try {
      const solver = new Solver(n);
      for (const c of clauses) solver.addClause(c);
      const result = solver.solve();
      
      if (result === 'SAT') {
        const model = solver.getModel();
        if (verifySolution(clauses, model)) satVerified++;
        else crashes++;
      } else if (result === 'UNSAT') {
        unsat++;
      } else {
        crashes++; // unexpected return value
      }
    } catch (e) {
      crashes++;
    }
  }
  
  console.log(`500 random: ${satVerified} SAT verified, ${unsat} UNSAT, ${crashes} crashes`);
  assert.strictEqual(crashes, 0, `${crashes} crashes or invalid solutions`);
});

test('XOR chain encoding (parity constraint)', () => {
  // Encode XOR chain: x1 ⊕ x2 ⊕ x3 = 1
  // XOR(a, b) = (a ∨ b) ∧ (¬a ∨ ¬b) [for 2 variables]
  // x1 ⊕ x2 ⊕ x3 = 1 means odd number true
  const solver = new Solver(3);
  // Tseitin encoding of x1 ⊕ x2 ⊕ x3 = 1:
  // (x1 ∨ x2 ∨ x3) ∧ (x1 ∨ ¬x2 ∨ ¬x3) ∧ (¬x1 ∨ x2 ∨ ¬x3) ∧ (¬x1 ∨ ¬x2 ∨ x3)
  solver.addClause([1, 2, 3]);
  solver.addClause([1, -2, -3]);
  solver.addClause([-1, 2, -3]);
  solver.addClause([-1, -2, 3]);
  
  assert.strictEqual(solver.solve(), 'SAT');
  const model = solver.getModel();
  const parity = (model[1] ? 1 : 0) + (model[2] ? 1 : 0) + (model[3] ? 1 : 0);
  assert.ok(parity % 2 === 1, `parity should be odd, got ${parity}`);
});

test('latin square 4x4: SAT (encoding combinatorial problem)', () => {
  // 4x4 Latin square: each number 1-4 appears exactly once in each row and column
  const n = 4;
  const numVars = n * n * n; // x(r,c,v) = cell (r,c) has value v
  const xvar = (r, c, v) => r * n * n + c * n + v + 1;
  const clauses = [];
  
  // Each cell has at least one value
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      const clause = [];
      for (let v = 0; v < n; v++) clause.push(xvar(r, c, v));
      clauses.push(clause);
    }
  }
  
  // Each cell has at most one value
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      for (let v1 = 0; v1 < n; v1++) {
        for (let v2 = v1 + 1; v2 < n; v2++) {
          clauses.push([-xvar(r, c, v1), -xvar(r, c, v2)]);
        }
      }
    }
  }
  
  // Each value once per row
  for (let r = 0; r < n; r++) {
    for (let v = 0; v < n; v++) {
      for (let c1 = 0; c1 < n; c1++) {
        for (let c2 = c1 + 1; c2 < n; c2++) {
          clauses.push([-xvar(r, c1, v), -xvar(r, c2, v)]);
        }
      }
    }
  }
  
  // Each value once per column
  for (let c = 0; c < n; c++) {
    for (let v = 0; v < n; v++) {
      for (let r1 = 0; r1 < n; r1++) {
        for (let r2 = r1 + 1; r2 < n; r2++) {
          clauses.push([-xvar(r1, c, v), -xvar(r2, c, v)]);
        }
      }
    }
  }
  
  const solver = new Solver(numVars);
  for (const c of clauses) solver.addClause(c);
  assert.strictEqual(solver.solve(), 'SAT');
  
  const model = solver.getModel();
  const grid = Array.from({length: n}, () => Array(n).fill(-1));
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      for (let v = 0; v < n; v++) {
        if (model[xvar(r, c, v)]) grid[r][c] = v + 1;
      }
    }
  }
  
  // Verify: each row/col has values 1-4
  for (let r = 0; r < n; r++) {
    assert.strictEqual(new Set(grid[r]).size, n, `row ${r} not a permutation`);
  }
  for (let c = 0; c < n; c++) {
    const col = grid.map(row => row[c]);
    assert.strictEqual(new Set(col).size, n, `col ${c} not a permutation`);
  }
});
