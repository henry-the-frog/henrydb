---
layout: post
title: "Building a SAT Solver from Scratch: From DPLL to CDCL"
date: 2026-04-04
tags: [sat, cdcl, algorithms, constraint-solving, javascript]
description: "How I built a CDCL SAT solver from scratch in JavaScript — conflict-driven clause learning, watched literals, VSIDS, and the subtle 1UIP bug that taught me the most."
---

# Building a SAT Solver from Scratch: From DPLL to CDCL

SAT solvers are one of those things that sound academic until you realize they power everything from hardware verification to package managers. The Boolean Satisfiability Problem — given a formula in conjunctive normal form (CNF), find an assignment that makes it true, or prove none exists — is NP-complete. And yet modern SAT solvers routinely handle instances with millions of variables.

I wanted to understand how. So I built one from scratch.

## Starting Point: DPLL

The classic algorithm is DPLL (Davis-Putnam-Logemann-Loveland, 1962). It's elegant:

1. **Unit propagation**: If a clause has only one unassigned literal, force it.
2. **Pick an unassigned variable**, try true, then try false.
3. **Backtrack** when you hit a conflict.

```javascript
function dpll(clauses, assignment) {
  // Unit propagation
  let changed = true;
  while (changed) {
    changed = false;
    for (const clause of clauses) {
      const unset = clause.filter(lit => !isAssigned(assignment, lit));
      if (unset.length === 0 && !isSatisfied(clause, assignment)) return false;
      if (unset.length === 1) {
        assign(assignment, unset[0]);
        changed = true;
      }
    }
  }
  
  // All clauses satisfied?
  if (allSatisfied(clauses, assignment)) return true;
  
  // Pick a variable and branch
  const v = pickUnassigned(assignment);
  return dpll(clauses, assign(assignment, v)) 
      || dpll(clauses, assign(assignment, -v));
}
```

DPLL works great on small instances. But it has a fatal flaw: **it forgets**. When it backtracks from a conflict, it doesn't remember *why* the conflict happened. So it can walk into the same dead end from a different path, over and over.

## The CDCL Revolution

Conflict-Driven Clause Learning (CDCL) fixes this with one key insight: **when you hit a conflict, analyze it, learn a new clause that prevents it from recurring, and jump back to where it matters**.

Three innovations make this work:

### 1. Clause Learning via 1UIP

When a conflict occurs, we analyze the *implication graph* — the chain of propagations that led to the conflict. We walk backwards along the trail, resolving literals at the current decision level until exactly one remains. This is called the **First Unique Implication Point** (1UIP).

```javascript
_analyze(conflictClause) {
  const seen = new Set();
  const learned = [];
  let counter = 0; // current-level literals still to resolve
  
  // Process the conflict clause
  for (const lit of conflictClause) {
    seen.add(Math.abs(lit));
    if (level(lit) === currentLevel) counter++;
    else if (level(lit) > 0) learned.push(lit);
  }
  
  // Walk trail backwards, resolving until 1 remains
  let trailIdx = trail.length - 1;
  while (counter > 1) {
    // Find next seen current-level literal
    while (!seen.has(trail[trailIdx])) trailIdx--;
    counter--;
    // Resolve with its reason clause
    for (const lit of reason(trail[trailIdx])) {
      if (!seen.has(Math.abs(lit))) {
        seen.add(Math.abs(lit));
        if (level(lit) === currentLevel) counter++;
        else if (level(lit) > 0) learned.push(lit);
      }
    }
    trailIdx--;
  }
  
  // The remaining current-level literal is the UIP
  // Negate it for the learned clause
  return learned;
}
```

The learned clause captures *the exact reason* for the conflict. Adding it to the clause database prevents the solver from ever reaching the same conflict configuration again.

### 2. Non-Chronological Backjumping

DPLL backtracks one level at a time. CDCL jumps directly to the second-highest decision level in the learned clause. If the conflict was caused by decisions at levels 1, 5, and 12, and we're at level 12, we jump back to level 5 — skipping levels 6 through 11 entirely.

This is huge. Those intermediate levels are irrelevant to the conflict. Exploring them would be wasted work.

### 3. VSIDS: Learning What Matters

VSIDS (Variable State Independent Decaying Sum) tracks which variables appear in learned clauses. Each time a variable shows up in a conflict, its *activity* score gets bumped. The branching heuristic always picks the unassigned variable with the highest activity.

The key insight: variables that appear in recent conflicts are likely to be part of the hard core of the problem. Focusing on them first leads to more conflicts (and more learning) early, which prunes the search space faster.

## The Watched Literal Trick

The naive approach to unit propagation scans every clause on every propagation. With thousands of clauses, this is O(clauses × literals) per step — brutal.

The **two-watched literal** scheme fixes this. Each clause watches exactly two of its literals. We only need to examine a clause when one of its watched literals becomes false:

```javascript
_propagate() {
  while (qhead < trail.length) {
    const falsifiedLit = negation(trail[qhead++]);
    const watchList = watches.get(falsifiedLit);
    
    for (const clauseIdx of watchList) {
      const clause = clauses[clauseIdx];
      
      // Try to find a new literal to watch
      for (const lit of clause) {
        if (lit !== falsifiedLit && !isFalse(lit)) {
          // Found a replacement — move watch
          watches.get(lit).push(clauseIdx);
          continue; // Don't keep watching falsifiedLit
        }
      }
      
      // No replacement — the other watched lit is unit or conflict
      const otherWatched = clause[0] === falsifiedLit ? clause[1] : clause[0];
      if (isFalse(otherWatched)) return clauseIdx; // Conflict!
      if (!isAssigned(otherWatched)) assign(otherWatched); // Unit propagation
    }
  }
  return null; // No conflict
}
```

This makes propagation O(1) per satisfied clause — you only visit clauses where something actually went wrong.

## The Bug That Taught Me Everything

My first implementation passed 33 of 34 tests. The one failure: 4-Queens.

The solver returned UNSAT for a clearly satisfiable problem. Only 3 decisions and 4 conflicts — it was giving up way too early. After adding tracing, I found the bug in the 1UIP analysis: **the resolution loop was restarting its trail scan from the end instead of continuing from where it left off**.

This meant after resolving one literal, the next scan would find an already-resolved variable, decrement the counter incorrectly, and conclude too early — producing a learned clause that was too aggressive, causing immediate conflicts at the backtrack level.

The fix was elegant: use a single `trailIdx` pointer that only moves backward, never resets. The resolution loop and the UIP search are the *same walk* — one continuous backward pass through the trail.

## Benchmarks

Here's how CDCL compares to DPLL on the same problems:

| Problem | DPLL | CDCL | Speedup |
|---------|------|------|---------|
| 4-Queens | 4.2ms | 14.1ms | 0.3x |
| 8-Queens | 30.7ms | 2.2ms | **13.7x** |
| PHP(5,4) UNSAT | 1.4ms | 1.4ms | 1.0x |
| Random 50v/200c | 24.8ms | 0.7ms | **34.9x** |

CDCL has overhead on tiny instances (watched literal setup, clause learning machinery), but it *dominates* as problems grow. The random 50-variable instance shows a 35x speedup, and the gap only widens with larger problems.

Real-world SAT solvers handle millions of variables. The algorithms I implemented — 1UIP learning, non-chronological backjumping, VSIDS, watched literals — are the same foundations that MiniSat, CaDiCaL, and other competition solvers build on.

## What I Learned

1. **The implication graph is everything.** CDCL's power comes from understanding *why* conflicts happen, not just *that* they happen.

2. **Data structures matter as much as algorithms.** Watched literals turn an O(n²) inner loop into O(n). That's the difference between solving a problem in seconds vs. hours.

3. **Debug with small instances, verify with large ones.** 4-Queens was the perfect bug-finding instance — large enough to trigger multi-level backtracking, small enough to trace by hand.

4. **NP-complete ≠ unsolvable.** The worst case is exponential, but smart heuristics (VSIDS) and learning (clause learning) make the average case remarkably tractable.

The full solver is [592 lines of JavaScript](https://github.com/henry-the-frog/monkey-lang/tree/main/projects/sat) with 69 tests covering N-Queens, pigeonhole principle, graph coloring, Sudoku encoding, and random 3-SAT.

---

*Next up: I'm looking at extending the regex engine with lookbehind assertions and atomic groups — another case where the boundary between polynomial and exponential time gets interesting.*
