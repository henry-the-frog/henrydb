'use strict';

// SAT Fuzzer: random 3-SAT at phase transition, verify solutions, brute-force cross-check
const { Solver, randomSAT, createSolver, TRUE, FALSE, UNDEF } = require('./solver.cjs');

// Brute-force solver for small instances (≤20 vars)
function bruteForce(numVars, clauses) {
  for (let mask = 0; mask < (1 << numVars); mask++) {
    let allSat = true;
    for (const clause of clauses) {
      let clauseSat = false;
      for (const lit of clause) {
        const v = Math.abs(lit);
        const positive = (mask >> (v - 1)) & 1;
        if ((lit > 0 && positive) || (lit < 0 && !positive)) {
          clauseSat = true;
          break;
        }
      }
      if (!clauseSat) { allSat = false; break; }
    }
    if (allSat) {
      // Return assignment
      const assigns = new Array(numVars + 1).fill(0);
      for (let v = 1; v <= numVars; v++) {
        assigns[v] = (mask >> (v - 1)) & 1 ? TRUE : FALSE;
      }
      return { sat: true, assigns };
    }
  }
  return { sat: false };
}

// Verify a solution satisfies all clauses
function verifySolution(assigns, clauses) {
  for (let i = 0; i < clauses.length; i++) {
    let satisfied = false;
    for (const lit of clauses[i]) {
      const v = Math.abs(lit);
      if ((lit > 0 && assigns[v] === TRUE) || (lit < 0 && assigns[v] === FALSE)) {
        satisfied = true;
        break;
      }
    }
    if (!satisfied) return { valid: false, failedClause: i };
  }
  return { valid: true };
}

// Seeded random
function seeded(seed) {
  let s = seed;
  return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}

function randomSATSeeded(rng, numVars, numClauses, clauseLen = 3) {
  const clauses = [];
  for (let i = 0; i < numClauses; i++) {
    const clause = [];
    const used = new Set();
    while (clause.length < clauseLen) {
      const v = Math.floor(rng() * numVars) + 1;
      if (used.has(v)) continue;
      used.add(v);
      clause.push(rng() < 0.5 ? v : -v);
    }
    clauses.push(clause);
  }
  return { numVars, clauses };
}

// === Fuzz runs ===
let passed = 0, failed = 0, errors = 0;
const startTime = Date.now();

// 1. Small instances with brute-force cross-check
console.log('=== Phase 1: Brute-force cross-check (small instances) ===');
for (let seed = 1; seed <= 500; seed++) {
  const rng = seeded(seed);
  const numVars = 3 + Math.floor(rng() * 10); // 3-12 vars
  const ratio = 3.5 + rng() * 2; // ratio 3.5-5.5 (around phase transition)
  const numClauses = Math.floor(numVars * ratio);
  
  const problem = randomSATSeeded(rng, numVars, numClauses, 3);
  
  try {
    const solver = createSolver(problem);
    const cdclResult = solver ? solver.solve() : 'UNSAT';
    const cdclSat = cdclResult === 'SAT';
    const bfResult = bruteForce(numVars, problem.clauses);
    
    if (cdclSat !== bfResult.sat) {
      console.error(`MISMATCH seed=${seed} vars=${numVars} clauses=${numClauses}: CDCL=${cdclResult} BF=${bfResult.sat}`);
      failed++;
      continue;
    }
    
    if (cdclSat && solver) {
      // Verify CDCL solution
      const verify = verifySolution(solver.assigns, problem.clauses);
      if (!verify.valid) {
        console.error(`INVALID SOLUTION seed=${seed}: clause ${verify.failedClause} not satisfied`);
        failed++;
        continue;
      }
    }
    
    passed++;
  } catch (e) {
    console.error(`ERROR seed=${seed}: ${e.message}`);
    errors++;
  }
  
  if (seed % 100 === 0) process.stderr.write(`  ${seed}/500 (${passed} pass, ${failed} fail)\n`);
}

// 2. Medium instances (20-50 vars) — solution verification only
console.log('\n=== Phase 2: Solution verification (medium instances) ===');
for (let seed = 1; seed <= 500; seed++) {
  const rng = seeded(seed + 10000);
  const numVars = 20 + Math.floor(rng() * 30); // 20-50 vars
  const ratio = 3.5 + rng() * 2;
  const numClauses = Math.floor(numVars * ratio);
  
  const problem = randomSATSeeded(rng, numVars, numClauses, 3);
  
  try {
    const solver = createSolver(problem);
    const result = solver ? solver.solve() : 'UNSAT';
    
    if (result === 'SAT' && solver) {
      const verify = verifySolution(solver.assigns, problem.clauses);
      if (!verify.valid) {
        console.error(`INVALID SOLUTION seed=${seed+10000} vars=${numVars}: clause ${verify.failedClause} not satisfied`);
        failed++;
        continue;
      }
    }
    
    passed++;
  } catch (e) {
    console.error(`ERROR seed=${seed+10000}: ${e.message}`);
    errors++;
  }
  
  if (seed % 100 === 0) process.stderr.write(`  ${seed}/500 (${passed} pass, ${failed} fail)\n`);
}

// 3. Stress: larger instances (100+ vars)
console.log('\n=== Phase 3: Stress test (large instances) ===');
for (let seed = 1; seed <= 100; seed++) {
  const rng = seeded(seed + 20000);
  const numVars = 50 + Math.floor(rng() * 150); // 50-200 vars
  const ratio = 4.0 + rng() * 1; // closer to phase transition
  const numClauses = Math.floor(numVars * ratio);
  
  const problem = randomSATSeeded(rng, numVars, numClauses, 3);
  
  try {
    const solver = createSolver(problem);
    if (!solver) { passed++; continue; }
    
    const result = solver.solve(5000); // timeout 5000 conflicts
    
    if (result === 'SAT' && solver) {
      const verify = verifySolution(solver.assigns, problem.clauses);
      if (!verify.valid) {
        console.error(`INVALID SOLUTION seed=${seed+20000} vars=${numVars}: clause ${verify.failedClause}`);
        failed++;
        continue;
      }
    }
    
    passed++;
  } catch (e) {
    console.error(`ERROR seed=${seed+20000}: ${e.message}`);
    errors++;
  }
  
  if (seed % 25 === 0) process.stderr.write(`  ${seed}/100 (${passed} pass, ${failed} fail)\n`);
}

const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
console.log(`\n=== SAT Fuzzer Results ===`);
console.log(`Passed: ${passed}, Failed: ${failed}, Errors: ${errors}, Time: ${elapsed}s`);
console.log(failed + errors === 0 ? 'ALL PASS ✓' : 'FAILURES DETECTED');
process.exit(failed + errors > 0 ? 1 : 0);
