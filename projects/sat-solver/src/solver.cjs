'use strict';

// ============================================================
// CDCL SAT Solver — Conflict-Driven Clause Learning
// ============================================================
// Architecture:
//   - Variables: 1-based integers (1..n)
//   - Literals: positive = variable true, negative = variable false
//   - Clauses: arrays of literals with 2-watched-literal scheme
//   - Trail: assignment stack with decision levels
//   - Conflict analysis: 1UIP (MiniSat-style single backward walk)
//   - Decision heuristic: VSIDS with geometric decay
// ============================================================

const UNDEF = 0;  // unassigned
const TRUE = 1;
const FALSE = -1;

class Clause {
  constructor(lits, learned = false) {
    this.lits = lits;       // mutable array of literals
    this.learned = learned;
    this.activity = 0;      // for clause deletion heuristic
    this.lbd = Infinity;    // Literal Block Distance (clause quality metric)
  }
}

class Solver {
  constructor(numVars = 0) {
    this.numVars = numVars;
    this._unsat = false;  // trivially UNSAT from contradictory unit clauses

    // Assignment: assigns[v] = TRUE/FALSE/UNDEF for variable v (1-based)
    this.assigns = new Int8Array(numVars + 1);

    // Decision level for each variable
    this.level = new Int32Array(numVars + 1);

    // Reason clause for each variable (null = decision or unit from input)
    this.reason = new Array(numVars + 1).fill(null);

    // Trail: ordered list of assigned literals
    this.trail = [];
    this.trailLim = [];  // trail index at each decision level start
    this.qhead = 0;      // propagation queue head

    // Clauses
    this.clauses = [];   // original clauses
    this.learneds = [];  // learned clauses

    // 2-Watched Literals: watchList[lit] = list of clauses watching this literal
    // lit encoding: positive lit L → index 2*L, negative lit -L → index 2*L+1
    // But simpler: use Map or offset arrays
    // We'll use: watches[litIndex(lit)] = [clause, ...]
    this.watches = new Array(2 * numVars + 2).fill(null);
    for (let i = 0; i < this.watches.length; i++) this.watches[i] = [];

    // VSIDS
    this.activity = new Float64Array(numVars + 1);
    this.activityInc = 1.0;
    this.activityDecay = 0.95;

    // Phase saving
    this.polarity = new Int8Array(numVars + 1);  // last polarity

    // Stats
    this.conflicts = 0;
    this.decisions = 0;
    this.propagations = 0;

    // Restart — Luby sequence
    this._lubyIndex = 0;
    this._lubyUnit = 100;  // base unit for Luby restarts

    // Clause deletion — LBD-based
    this.maxLearneds = 2000;  // start higher, use LBD to manage
    this.learnedInc = 1.1;
    this._glueKeepLimit = 3;  // clauses with LBD <= 3 are never deleted ("glue clauses")
  }

  // Luby sequence: 1, 1, 2, 1, 1, 2, 4, 1, 1, 2, 1, 1, 2, 4, 8, ...
  static _luby(i) {
    let size = 1, seq = 0;
    while (size < i + 1) {
      size = 2 * size + 1;
      seq++;
    }
    while (size - 1 !== i) {
      size = (size - 1) >> 1;
      seq--;
      if (i >= size) {
        i -= size;
      }
    }
    return 1 << seq;
  }

  // Literal → watch list index
  _litIndex(lit) {
    return lit > 0 ? 2 * lit : 2 * (-lit) + 1;
  }

  // Get value of a literal under current assignment
  _litValue(lit) {
    const v = Math.abs(lit);
    const a = this.assigns[v];
    if (a === UNDEF) return UNDEF;
    return (lit > 0 ? a : -a);
  }

  // Current decision level
  _decisionLevel() {
    return this.trailLim.length;
  }

  // Add a clause (during problem setup or learning)
  addClause(lits) {
    if (lits.length === 0) return false;  // empty clause = UNSAT
    if (lits.length === 1) {
      // Unit clause: enqueue immediately
      if (!this._enqueue(lits[0], null)) {
        this._unsat = true;
        return false;
      }
      return true;
    }

    const clause = new Clause([...lits]);
    this.clauses.push(clause);
    this._watch(clause);
    return true;
  }

  _addLearned(lits) {
    const clause = new Clause([...lits], true);
    this.learneds.push(clause);
    if (lits.length >= 2) {
      this._watch(clause);
    }
    return clause;
  }

  _watch(clause) {
    // Watch the first two literals
    this.watches[this._litIndex(clause.lits[0])].push(clause);
    if (clause.lits.length >= 2) {
      this.watches[this._litIndex(clause.lits[1])].push(clause);
    }
  }

  // Assign a literal (enqueue on trail)
  _enqueue(lit, reason) {
    const v = Math.abs(lit);
    if (this.assigns[v] !== UNDEF) {
      return this._litValue(lit) === TRUE;  // already assigned correctly?
    }
    this.assigns[v] = lit > 0 ? TRUE : FALSE;
    this.level[v] = this._decisionLevel();
    this.reason[v] = reason;
    this.trail.push(lit);
    return true;
  }

  // Boolean Constraint Propagation using 2-watched literals
  _propagate() {
    let conflict = null;

    while (this.qhead < this.trail.length) {
      const p = this.trail[this.qhead++];  // literal that just became true
      this.propagations++;
      const falseLit = -p;  // the negation became false
      const idx = this._litIndex(falseLit);
      const watchList = this.watches[idx];
      const newWatchList = [];

      for (let i = 0; i < watchList.length; i++) {
        const clause = watchList[i];
        const lits = clause.lits;

        // Make sure falseLit is at position 1
        if (lits[0] === falseLit) {
          lits[0] = lits[1];
          lits[1] = falseLit;
        }

        // If first watched literal is true, clause is satisfied
        if (this._litValue(lits[0]) === TRUE) {
          newWatchList.push(clause);
          continue;
        }

        // Try to find a new literal to watch (replace position 1)
        let found = false;
        for (let j = 2; j < lits.length; j++) {
          if (this._litValue(lits[j]) !== FALSE) {
            // Swap lits[1] and lits[j]
            lits[1] = lits[j];
            lits[j] = falseLit;
            // Add to new literal's watch list
            this.watches[this._litIndex(lits[1])].push(clause);
            found = true;
            break;
          }
        }

        if (found) continue;

        // No replacement found — clause is unit or conflict
        newWatchList.push(clause);  // keep watching

        if (this._litValue(lits[0]) === FALSE) {
          // Conflict!
          conflict = clause;
          // Copy remaining watches
          for (let j = i + 1; j < watchList.length; j++) {
            newWatchList.push(watchList[j]);
          }
          break;
        } else {
          // Unit propagation: lits[0] is the only non-false literal
          if (!this._enqueue(lits[0], clause)) {
            conflict = clause;
            for (let j = i + 1; j < watchList.length; j++) {
              newWatchList.push(watchList[j]);
            }
            break;
          }
        }
      }

      this.watches[idx] = newWatchList;
    }

    return conflict;
  }

  // 1UIP Conflict Analysis (MiniSat-style)
  _analyze(conflict) {
    const seen = new Uint8Array(this.numVars + 1);
    const learnt = [];
    let counter = 0;      // unresolved current-level literals
    let btLevel = 0;      // backtrack level
    let trailIdx = this.trail.length - 1;
    let reason = conflict;

    // Process conflict clause, then walk trail backward resolving
    do {
      // Add all literals from reason to the frontier
      for (const lit of reason.lits) {
        const v = Math.abs(lit);
        if (seen[v]) continue;
        seen[v] = 1;
        this._bumpActivity(v);
        if (this.level[v] === this._decisionLevel()) {
          counter++;
        } else if (this.level[v] > 0) {
          learnt.push(lit);  // literal is false at this level — keep it
          if (this.level[v] > btLevel) btLevel = this.level[v];
        }
      }

      // Walk backward to find next seen current-level literal
      while (trailIdx >= 0 && !seen[Math.abs(this.trail[trailIdx])]) {
        trailIdx--;
      }
      const p = this.trail[trailIdx];
      trailIdx--;
      counter--;

      if (counter <= 0) {
        // p is the 1UIP — add its negation as asserting literal
        learnt.unshift(-p);
        break;
      }

      // Resolve with p's reason
      reason = this.reason[Math.abs(p)];
      if (!reason) {
        // p is a decision with no reason — it IS the UIP
        learnt.unshift(-p);
        break;
      }
    } while (true);

    // Clause minimization
    const minimized = this._minimizeClause(learnt, seen);

    // Ensure the literal at btLevel is at position 1 (for watched literals)
    if (minimized.length >= 2) {
      let maxIdx = 1;
      let maxLevel = this.level[Math.abs(minimized[1])];
      for (let i = 2; i < minimized.length; i++) {
        const lv = this.level[Math.abs(minimized[i])];
        if (lv > maxLevel) {
          maxLevel = lv;
          maxIdx = i;
        }
      }
      if (maxIdx !== 1) {
        const tmp = minimized[1];
        minimized[1] = minimized[maxIdx];
        minimized[maxIdx] = tmp;
      }
    }

    // Compute LBD (Literal Block Distance) — count distinct decision levels
    const levelSet = new Set();
    for (const lit of minimized) {
      const v = Math.abs(lit);
      if (this.level[v] > 0) levelSet.add(this.level[v]);
    }
    const lbd = levelSet.size;

    return { learnt: minimized, btLevel, lbd };
  }

  _minimizeClause(learnt, seen) {
    // Simple minimization: remove literals whose reason is subsumed
    if (learnt.length <= 2) return learnt;

    const dominated = new Set();
    for (let i = 1; i < learnt.length; i++) {
      const v = Math.abs(learnt[i]);
      const r = this.reason[v];
      if (!r) continue;
      // Check if all literals in reason (except v) are either:
      // - in learnt clause, or
      // - at decision level 0
      let dominated_flag = true;
      for (const lit of r.lits) {
        const rv = Math.abs(lit);
        if (rv === v) continue;
        if (this.level[rv] === 0) continue;
        // Check if rv is in learnt (via seen array or linear scan)
        let inLearnt = false;
        for (const l of learnt) {
          if (Math.abs(l) === rv) { inLearnt = true; break; }
        }
        if (!inLearnt) { dominated_flag = false; break; }
      }
      if (dominated_flag) dominated.add(i);
    }

    if (dominated.size === 0) return learnt;
    return learnt.filter((_, i) => !dominated.has(i));
  }

  // VSIDS: bump variable activity
  _bumpActivity(v) {
    this.activity[v] += this.activityInc;
    // Rescale if overflow
    if (this.activity[v] > 1e100) {
      for (let i = 1; i <= this.numVars; i++) {
        this.activity[i] *= 1e-100;
      }
      this.activityInc *= 1e-100;
    }
  }

  _decayActivity() {
    this.activityInc /= this.activityDecay;
  }

  // Pick next unassigned variable (VSIDS)
  _pickBranchVar() {
    let best = -1;
    let bestAct = -1;
    for (let v = 1; v <= this.numVars; v++) {
      if (this.assigns[v] === UNDEF && this.activity[v] > bestAct) {
        bestAct = this.activity[v];
        best = v;
      }
    }
    return best;
  }

  // Backtrack to given level
  _backtrack(level) {
    if (this._decisionLevel() <= level) return;

    for (let i = this.trail.length - 1; i >= this.trailLim[level]; i--) {
      const v = Math.abs(this.trail[i]);
      this.polarity[v] = this.assigns[v];  // save phase
      this.assigns[v] = UNDEF;
      this.reason[v] = null;
      this.level[v] = -1;
    }
    this.trail.length = this.trailLim[level];
    this.trailLim.length = level;
    this.qhead = this.trail.length;
  }

  // Main solve loop
  solve() {
    if (this._unsat) return 'UNSAT';

    // Initial propagation (unit clauses)
    let conflict = this._propagate();
    if (conflict) return 'UNSAT';

    while (true) {
      conflict = this._propagate();

      if (conflict) {
        this.conflicts++;

        if (this._decisionLevel() === 0) return 'UNSAT';

        // Conflict analysis
        const { learnt, btLevel, lbd } = this._analyze(conflict);
        this._decayActivity();

        // Backtrack
        this._backtrack(btLevel);

        // Add learned clause
        if (learnt.length === 1) {
          this._enqueue(learnt[0], null);
        } else {
          const clause = this._addLearned(learnt);
          clause.lbd = lbd;
          this._enqueue(learnt[0], clause);
        }

        // Luby restart check
        const lubyLimit = Solver._luby(this._lubyIndex) * this._lubyUnit;
        if (this.conflicts >= lubyLimit) {
          this._lubyIndex++;
          this._backtrack(0);
          conflict = this._propagate();
          if (conflict) return 'UNSAT';
        }

        // Clause deletion (LBD-aware)
        if (this.learneds.length > this.maxLearneds + this.trail.length) {
          this._reduceLearneds();
        }

        continue;
      }

      // No conflict — make a decision
      const v = this._pickBranchVar();
      if (v === -1) return 'SAT';  // all variables assigned

      this.decisions++;
      this.trailLim.push(this.trail.length);

      // Use saved polarity (phase saving)
      const pol = this.polarity[v] === FALSE ? -v : v;
      this._enqueue(pol, null);
    }
  }

  // Get satisfying assignment (call after solve returns 'SAT')
  getModel() {
    const model = {};
    for (let v = 1; v <= this.numVars; v++) {
      model[v] = this.assigns[v] === TRUE;
    }
    return model;
  }

  // Clause database reduction (LBD-aware)
  _reduceLearneds() {
    // Separate glue clauses (LBD <= limit) from removable
    const glue = [];
    const removable = [];
    for (const c of this.learneds) {
      if (c.lbd <= this._glueKeepLimit) {
        glue.push(c);
      } else {
        removable.push(c);
      }
    }

    // Sort removable by activity, remove bottom half
    removable.sort((a, b) => a.activity - b.activity);
    const keep = Math.floor(removable.length / 2);
    const kept = removable.slice(removable.length - keep);

    this.learneds = [...glue, ...kept];

    // Rebuild watch lists
    this._rebuildWatches();

    this.maxLearneds = Math.floor(this.maxLearneds * this.learnedInc);
  }

  // Preprocessing: subsumption elimination + failed literal probing
  preprocess() {
    // Sort clauses by size (small clauses subsume large ones)
    this.clauses.sort((a, b) => a.lits.length - b.lits.length);

    // Forward subsumption: if clause A ⊆ clause B, remove B
    const litSets = this.clauses.map(c => new Set(c.lits));
    const keep = new Array(this.clauses.length).fill(true);

    for (let i = 0; i < this.clauses.length; i++) {
      if (!keep[i]) continue;
      const setI = litSets[i];
      for (let j = i + 1; j < this.clauses.length; j++) {
        if (!keep[j]) continue;
        if (setI.size > litSets[j].size) continue;
        // Check if setI ⊆ setJ
        let subsumed = true;
        for (const lit of setI) {
          if (!litSets[j].has(lit)) { subsumed = false; break; }
        }
        if (subsumed) keep[j] = false;
      }
    }

    const removed = this.clauses.filter((_, i) => !keep[i]).length;
    this.clauses = this.clauses.filter((_, i) => keep[i]);

    // Rebuild watches after removing clauses
    if (removed > 0) this._rebuildWatches();

    return { removed };
  }

  // Failed literal probing: try assigning each unassigned literal,
  // propagate, and if conflict → the opposite must be true
  probe() {
    let forced = 0;
    let changed = true;
    while (changed) {
      changed = false;
      for (let v = 1; v <= this.numVars; v++) {
        if (this.assigns[v] !== UNDEF) continue;

        for (const pol of [1, -1]) {
          const lit = pol * v;
          // Save state
          const savedTrail = this.trail.length;
          const savedQhead = this.qhead;
          this.trailLim.push(this.trail.length);

          this._enqueue(lit, null);
          const conflict = this._propagate();

          // Undo
          for (let i = this.trail.length - 1; i >= savedTrail; i--) {
            const tv = Math.abs(this.trail[i]);
            this.assigns[tv] = UNDEF;
            this.reason[tv] = null;
            this.level[tv] = -1;
          }
          this.trail.length = savedTrail;
          this.trailLim.pop();
          this.qhead = savedQhead;

          if (conflict) {
            // lit leads to conflict → must assign -lit
            this._enqueue(-lit, null);
            const c2 = this._propagate();
            if (c2) {
              this._unsat = true;
              return { forced, unsat: true };
            }
            forced++;
            changed = true;
            break;
          }
        }
      }
    }
    return { forced, unsat: false };
  }

  _rebuildWatches() {
    for (let i = 0; i < this.watches.length; i++) {
      this.watches[i] = [];
    }
    for (const c of this.clauses) {
      if (c.lits.length >= 2) this._watch(c);
    }
    for (const c of this.learneds) {
      if (c.lits.length >= 2) this._watch(c);
    }
  }

  getStats() {
    return {
      variables: this.numVars,
      originalClauses: this.clauses.length,
      learnedClauses: this.learneds.length,
      conflicts: this.conflicts,
      decisions: this.decisions,
      propagations: this.propagations,
    };
  }
}

// ============================================================
// DIMACS CNF Parser
// ============================================================
function parseDIMACS(text) {
  const lines = text.split('\n');
  let numVars = 0, numClauses = 0;
  const clauses = [];
  let currentClause = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('c')) continue;  // comment
    if (trimmed.startsWith('p')) {
      const parts = trimmed.split(/\s+/);
      numVars = parseInt(parts[2]);
      numClauses = parseInt(parts[3]);
      continue;
    }

    // Clause literals
    const nums = trimmed.split(/\s+/).map(Number);
    for (const n of nums) {
      if (n === 0) {
        if (currentClause.length > 0) {
          clauses.push(currentClause);
          currentClause = [];
        }
      } else {
        currentClause.push(n);
      }
    }
  }
  if (currentClause.length > 0) clauses.push(currentClause);

  return { numVars, numClauses, clauses };
}

// ============================================================
// Problem Encoders
// ============================================================

// Pigeonhole: n+1 pigeons into n holes (always UNSAT)
function encodePigeonhole(n) {
  const pigeons = n + 1;
  const holes = n;
  // Variable (p, h) = pigeon p is in hole h = p * holes + h + 1
  const numVars = pigeons * holes;
  const clauses = [];

  const v = (p, h) => p * holes + h + 1;

  // Every pigeon must be in some hole
  for (let p = 0; p < pigeons; p++) {
    const clause = [];
    for (let h = 0; h < holes; h++) {
      clause.push(v(p, h));
    }
    clauses.push(clause);
  }

  // No two pigeons in the same hole
  for (let h = 0; h < holes; h++) {
    for (let p1 = 0; p1 < pigeons; p1++) {
      for (let p2 = p1 + 1; p2 < pigeons; p2++) {
        clauses.push([-v(p1, h), -v(p2, h)]);
      }
    }
  }

  return { numVars, clauses };
}

// N-Queens
function encodeNQueens(n) {
  // Variable q(r, c) = queen at row r, col c = r * n + c + 1
  const numVars = n * n;
  const clauses = [];
  const v = (r, c) => r * n + c + 1;

  // At least one queen per row
  for (let r = 0; r < n; r++) {
    const clause = [];
    for (let c = 0; c < n; c++) clause.push(v(r, c));
    clauses.push(clause);
  }

  // At most one queen per row
  for (let r = 0; r < n; r++) {
    for (let c1 = 0; c1 < n; c1++) {
      for (let c2 = c1 + 1; c2 < n; c2++) {
        clauses.push([-v(r, c1), -v(r, c2)]);
      }
    }
  }

  // At most one queen per column
  for (let c = 0; c < n; c++) {
    for (let r1 = 0; r1 < n; r1++) {
      for (let r2 = r1 + 1; r2 < n; r2++) {
        clauses.push([-v(r1, c), -v(r2, c)]);
      }
    }
  }

  // At most one queen per diagonal (↘)
  for (let d = -(n - 1); d < n; d++) {
    const cells = [];
    for (let r = 0; r < n; r++) {
      const c = r - d;
      if (c >= 0 && c < n) cells.push(v(r, c));
    }
    for (let i = 0; i < cells.length; i++) {
      for (let j = i + 1; j < cells.length; j++) {
        clauses.push([-cells[i], -cells[j]]);
      }
    }
  }

  // At most one queen per anti-diagonal (↗)
  for (let d = 0; d < 2 * n - 1; d++) {
    const cells = [];
    for (let r = 0; r < n; r++) {
      const c = d - r;
      if (c >= 0 && c < n) cells.push(v(r, c));
    }
    for (let i = 0; i < cells.length; i++) {
      for (let j = i + 1; j < cells.length; j++) {
        clauses.push([-cells[i], -cells[j]]);
      }
    }
  }

  return { numVars, clauses, decode: (model) => {
    const queens = [];
    for (let r = 0; r < n; r++) {
      for (let c = 0; c < n; c++) {
        if (model[v(r, c)]) queens.push([r, c]);
      }
    }
    return queens;
  }};
}

// Graph Coloring
function encodeGraphColoring(numNodes, edges, numColors) {
  // Variable (node, color) = node * numColors + color + 1
  const numVars = numNodes * numColors;
  const clauses = [];
  const v = (node, color) => node * numColors + color + 1;

  // Each node has at least one color
  for (let node = 0; node < numNodes; node++) {
    const clause = [];
    for (let c = 0; c < numColors; c++) clause.push(v(node, c));
    clauses.push(clause);
  }

  // Each node has at most one color
  for (let node = 0; node < numNodes; node++) {
    for (let c1 = 0; c1 < numColors; c1++) {
      for (let c2 = c1 + 1; c2 < numColors; c2++) {
        clauses.push([-v(node, c1), -v(node, c2)]);
      }
    }
  }

  // Adjacent nodes have different colors
  for (const [a, b] of edges) {
    for (let c = 0; c < numColors; c++) {
      clauses.push([-v(a, c), -v(b, c)]);
    }
  }

  return { numVars, clauses, decode: (model) => {
    const coloring = {};
    for (let node = 0; node < numNodes; node++) {
      for (let c = 0; c < numColors; c++) {
        if (model[v(node, c)]) coloring[node] = c;
      }
    }
    return coloring;
  }};
}

// Sudoku
function encodeSudoku(grid) {
  // grid: 9x9 array, 0 = empty, 1-9 = filled
  const n = 9;
  const numVars = n * n * n;  // (row, col, digit)
  const clauses = [];
  const v = (r, c, d) => r * n * n + c * n + d + 1;  // d is 0-based (0=digit 1, 8=digit 9)

  // Each cell has at least one digit
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      const clause = [];
      for (let d = 0; d < n; d++) clause.push(v(r, c, d));
      clauses.push(clause);
    }
  }

  // Each cell has at most one digit
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      for (let d1 = 0; d1 < n; d1++) {
        for (let d2 = d1 + 1; d2 < n; d2++) {
          clauses.push([-v(r, c, d1), -v(r, c, d2)]);
        }
      }
    }
  }

  // Each row has each digit
  for (let r = 0; r < n; r++) {
    for (let d = 0; d < n; d++) {
      const clause = [];
      for (let c = 0; c < n; c++) clause.push(v(r, c, d));
      clauses.push(clause);
    }
  }

  // Each column has each digit
  for (let c = 0; c < n; c++) {
    for (let d = 0; d < n; d++) {
      const clause = [];
      for (let r = 0; r < n; r++) clause.push(v(r, c, d));
      clauses.push(clause);
    }
  }

  // Each 3x3 box has each digit
  for (let br = 0; br < 3; br++) {
    for (let bc = 0; bc < 3; bc++) {
      for (let d = 0; d < n; d++) {
        const clause = [];
        for (let r = br * 3; r < br * 3 + 3; r++) {
          for (let c = bc * 3; c < bc * 3 + 3; c++) {
            clause.push(v(r, c, d));
          }
        }
        clauses.push(clause);
      }
    }
  }

  // Given clues
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      if (grid[r][c] !== 0) {
        clauses.push([v(r, c, grid[r][c] - 1)]);  // unit clause
      }
    }
  }

  return { numVars, clauses, decode: (model) => {
    const result = Array.from({ length: n }, () => new Array(n).fill(0));
    for (let r = 0; r < n; r++) {
      for (let c = 0; c < n; c++) {
        for (let d = 0; d < n; d++) {
          if (model[v(r, c, d)]) result[r][c] = d + 1;
        }
      }
    }
    return result;
  }};
}

// Random 3-SAT generator
function randomSAT(numVars, numClauses, clauseLen = 3) {
  const clauses = [];
  for (let i = 0; i < numClauses; i++) {
    const clause = [];
    const used = new Set();
    while (clause.length < clauseLen) {
      const v = Math.floor(Math.random() * numVars) + 1;
      if (used.has(v)) continue;
      used.add(v);
      clause.push(Math.random() < 0.5 ? v : -v);
    }
    clauses.push(clause);
  }
  return { numVars, clauses };
}

// Helper: create solver from problem encoding
function createSolver(problem) {
  const solver = new Solver(problem.numVars);
  for (const clause of problem.clauses) {
    if (!solver.addClause(clause)) return null;  // trivially UNSAT
  }
  return solver;
}

module.exports = {
  Solver,
  Clause,
  parseDIMACS,
  encodePigeonhole,
  encodeNQueens,
  encodeGraphColoring,
  encodeSudoku,
  randomSAT,
  createSolver,
  TRUE, FALSE, UNDEF
};
