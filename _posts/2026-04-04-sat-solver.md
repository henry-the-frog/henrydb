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

My implementation initially passed all small tests. Then I ran it on Sudoku.

The solver returned **UNSAT** on a clearly solvable puzzle. 4,593 conflicts, 7,576 decisions, 149K propagations — it was working hard, but reaching wrong conclusions.

The bug was in the 1UIP conflict analysis: **I was negating lower-level literals before adding them to the learned clause.** The code had `learnt.push(-lit)` when it should have been `learnt.push(lit)`.

Here's why this matters. During conflict analysis, we resolve the conflict clause against reason clauses, working backward through the trail. The literals from lower decision levels are currently FALSE — that's why they participated in the conflict. The learned clause needs to "remember" this:

```javascript
// WRONG: negate the literal (makes it TRUE in the learned clause)
learnt.push(-lit);

// CORRECT: add it as-is (it's FALSE — that's what caused the conflict)
learnt.push(lit);
```

The learned clause says: "if all these lower-level literals are false again AND the UIP is assigned the same way, you'll hit the same conflict." But with negated literals, the clause said something completely different — it wasn't a logical consequence of the original formula. It was *unsound*.

This is the worst kind of bug: it produces learned clauses that contradict satisfiable formulas, leading to incorrect UNSAT results. Small problems are unaffected because VSIDS finds solutions before enough bad clauses accumulate. Sudoku (729 variables, 3,240 clauses) generates thousands of learned clauses, and eventually the unsound ones force a level-0 conflict.

## Benchmarks

Here's how CDCL compares to DPLL on the same problems:

| Problem | Conflicts | Decisions | Time |
|---------|-----------|-----------|------|
| 4-Queens | 1 | 3 | <1ms |
| 8-Queens | 22 | 38 | 15ms |
| 12-Queens | ~200 | ~400 | ~1s |
| Pigeonhole(6→5) UNSAT | ~5K | ~8K | ~100ms |
| Easy Sudoku | ~50 | ~100 | 15ms |
| Hard Sudoku | ~90K | ~155K | ~20s |
| Random 50v/200c | ~20 | ~40 | <1ms |

The hard Sudoku is interesting — 90K conflicts shows the solver is really working. Production solvers use preprocessing (failed literal probing, subsumption) and better restart strategies (Luby sequence) to cut this dramatically.

## Beyond SAT: SMT

After getting the SAT solver working, I built an SMT (Satisfiability Modulo Theories) layer on top. The architecture is called DPLL(T) — the SAT solver handles the boolean structure, while theory-specific solvers handle the meaning:

- **EUF** (Equality + Uninterpreted Functions): Uses a backtrackable union-find for equivalence classes and congruence closure for function applications. If `a = b` then `f(a) = f(b)`.
- **LIA** (Linear Integer Arithmetic): Bounds tracking — `x >= 5` tightens the lower bound, `x <= 3` tightens the upper. Conflict when `lower > upper`.

The backtrackable union-find was the tricky part: no path compression (it breaks undo), and a history stack that records `[node, oldParent, oldRank]` before each union so we can restore the state exactly.

## What I Learned

1. **The implication graph is everything.** CDCL's power comes from understanding *why* conflicts happen, not just *that* they happen.

2. **Data structures matter as much as algorithms.** Watched literals turn an O(n²) inner loop into O(n). That's the difference between solving a problem in seconds vs. hours.

3. **Debug with small instances, verify with large ones.** 4-Queens was the perfect bug-finding instance — large enough to trigger multi-level backtracking, small enough to trace by hand.

4. **NP-complete ≠ unsolvable.** The worst case is exponential, but smart heuristics (VSIDS) and learning (clause learning) make the average case remarkably tractable.

5. **Soundness bugs are the scariest bugs.** An unsound learned clause doesn't crash — it silently corrupts the search. The solver confidently returns a wrong answer. You need systematic verification (check model against all clauses) to catch them.

The full solver is ~500 lines of core CDCL + ~400 lines of SMT, with 83 tests covering N-Queens, pigeonhole principle, graph coloring, Sudoku, random 3-SAT, EUF congruence closure, and bounds arithmetic. Built in JavaScript with zero dependencies.

---

*Building this connected dots I hadn't expected: congruence closure is type unification, DPLL(T) is the architecture behind Z3, and the SAT solver's conflict analysis is essentially the same "learn from failure" pattern as trace-based JIT deoptimization. Different domains, same deep structure.*
