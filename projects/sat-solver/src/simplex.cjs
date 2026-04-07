'use strict';

// ============================================================
// Simplex Solver for Linear Arithmetic
// ============================================================
// Implements the dual Simplex method for SMT-LIA:
//   - Tableau form with basic/non-basic variables
//   - Incremental bound assertion
//   - Bland's anti-cycling rule
//   - Conflict explanation via infeasible subset
//   - Backtrackable state
// ============================================================

class Simplex {
  constructor() {
    // Variables: original + slack
    this.vars = new Map();       // name → {index, lower, upper, value, basic}
    this.nextIdx = 0;
    this.rows = new Map();       // basic var index → Map(non-basic index → coefficient)
    this.history = [];           // for backtracking
    this.boundHistory = [];      // assertion history for explanation
  }

  // Register an original variable
  addVar(name) {
    if (this.vars.has(name)) return this.vars.get(name).index;
    const idx = this.nextIdx++;
    this.vars.set(name, {
      index: idx,
      lower: -Infinity,
      upper: Infinity,
      value: 0,
      basic: false,
      name,
    });
    return idx;
  }

  // Add a constraint: sum(coeffs[i] * vars[i]) <= bound
  // Creates a slack variable: slack = bound - sum(...)
  // slack >= 0
  addConstraint(terms, op, bound) {
    // terms: [{var: name, coeff: number}]
    // op: '<=' | '>=' | '='
    // Normalize to: slack = bound - sum(coeffs * vars) with slack >= 0 for <=

    // Ensure all variables exist
    for (const t of terms) this.addVar(t.var);

    const slackName = `__slack_${this.nextIdx}`;
    const slackIdx = this.addVar(slackName);
    const slackVar = this.vars.get(slackName);
    slackVar.basic = true;
    slackVar.value = bound;

    // Build row: slack = bound - sum(coeff_i * x_i)
    // In tableau: row maps non-basic vars to their coefficients
    const row = new Map();
    for (const t of terms) {
      const vi = this.vars.get(t.var);
      const coeff = -t.coeff;  // slack = bound - sum, so coefficient is negated
      if (coeff !== 0) row.set(vi.index, coeff);
      slackVar.value -= t.coeff * vi.value;  // adjust current value
    }
    this.rows.set(slackIdx, row);

    if (op === '<=') {
      slackVar.lower = 0;
    } else if (op === '>=') {
      slackVar.upper = 0;
    } else if (op === '=') {
      slackVar.lower = 0;
      slackVar.upper = 0;
    }

    return slackName;
  }

  // Assert a bound: x <= val or x >= val
  assertBound(name, op, val) {
    const v = this.vars.get(name);
    if (!v) throw new Error(`Unknown variable: ${name}`);

    // Save for backtrack
    this.boundHistory.push({ name, oldLower: v.lower, oldUpper: v.upper, oldValue: v.value });

    if (op === '<=' || op === 'le') {
      v.upper = Math.min(v.upper, val);
    } else if (op === '>=' || op === 'ge') {
      v.lower = Math.max(v.lower, val);
    } else if (op === '=' || op === 'eq') {
      v.lower = Math.max(v.lower, val);
      v.upper = Math.min(v.upper, val);
    }
  }

  // Check feasibility
  check() {
    // First: fix non-basic variables to satisfy their bounds
    for (const v of this.vars.values()) {
      if (v.basic) continue;
      if (v.lower > v.upper + 1e-10) {
        return { feasible: false, conflict: v.index };
      }
      if (v.value < v.lower) {
        this._updateNonBasic(v, v.lower);
      } else if (v.value > v.upper) {
        this._updateNonBasic(v, v.upper);
      }
    }

    const maxIterations = 1000;
    for (let iter = 0; iter < maxIterations; iter++) {
      // Find a basic variable violating its bounds
      let pivotRow = null;
      for (const [idx, row] of this.rows) {
        const v = this._varByIdx(idx);
        if (v.value < v.lower - 1e-10) {
          pivotRow = { idx, direction: 'increase' };
          break;  // Bland's rule: first violating
        }
        if (v.value > v.upper + 1e-10) {
          pivotRow = { idx, direction: 'decrease' };
          break;
        }
      }

      if (!pivotRow) return { feasible: true };

      // Find a non-basic variable to pivot with
      const row = this.rows.get(pivotRow.idx);
      let pivotCol = null;

      for (const [nbIdx, coeff] of row) {
        const nbVar = this._varByIdx(nbIdx);
        if (nbVar.basic) continue;  // shouldn't happen but safety

        if (pivotRow.direction === 'increase') {
          // Need to increase basic var: increase nb with positive coeff, or decrease nb with negative coeff
          if (coeff > 0 && nbVar.value < nbVar.upper - 1e-10) {
            pivotCol = { idx: nbIdx, coeff };
            break;  // Bland's rule
          }
          if (coeff < 0 && nbVar.value > nbVar.lower + 1e-10) {
            pivotCol = { idx: nbIdx, coeff };
            break;
          }
        } else {
          // Need to decrease basic var
          if (coeff < 0 && nbVar.value < nbVar.upper - 1e-10) {
            pivotCol = { idx: nbIdx, coeff };
            break;
          }
          if (coeff > 0 && nbVar.value > nbVar.lower + 1e-10) {
            pivotCol = { idx: nbIdx, coeff };
            break;
          }
        }
      }

      if (!pivotCol) {
        // No suitable pivot → infeasible
        return { feasible: false, conflict: pivotRow.idx };
      }

      // Perform pivot
      this._pivot(pivotRow.idx, pivotCol.idx, pivotCol.coeff);
    }

    return { feasible: false, reason: 'max iterations' };
  }

  _pivot(basicIdx, nbIdx, coeff) {
    const basicVar = this._varByIdx(basicIdx);
    const nbVar = this._varByIdx(nbIdx);

    // Update the value of nbVar
    const row = this.rows.get(basicIdx);
    let delta;
    if (basicVar.value < basicVar.lower) {
      delta = (basicVar.lower - basicVar.value) / coeff;
    } else {
      delta = (basicVar.upper - basicVar.value) / coeff;
    }

    // Update non-basic variable value
    nbVar.value += delta;

    // Update all basic variables that depend on nbIdx
    for (const [bIdx, bRow] of this.rows) {
      if (bRow.has(nbIdx)) {
        const bVar = this._varByIdx(bIdx);
        bVar.value += bRow.get(nbIdx) * delta;
      }
    }

    // Now swap: nbIdx becomes basic, basicIdx becomes non-basic
    // Rewrite basicIdx's row to express nbIdx
    // basicIdx = ... + coeff * nbIdx + ...
    // → nbIdx = (basicIdx - ...) / coeff
    const newRow = new Map();
    newRow.set(basicIdx, 1 / coeff);
    for (const [varIdx, c] of row) {
      if (varIdx === nbIdx) continue;
      newRow.set(varIdx, -c / coeff);
    }

    // Remove old row, add new
    this.rows.delete(basicIdx);
    this.rows.set(nbIdx, newRow);

    // Substitute nbIdx in all other rows
    for (const [bIdx, bRow] of this.rows) {
      if (bIdx === nbIdx) continue;
      if (!bRow.has(nbIdx)) continue;
      const c = bRow.get(nbIdx);
      bRow.delete(nbIdx);
      for (const [varIdx, nc] of newRow) {
        const old = bRow.get(varIdx) || 0;
        const newVal = old + c * nc;
        if (Math.abs(newVal) < 1e-15) bRow.delete(varIdx);
        else bRow.set(varIdx, newVal);
      }
    }

    // Update basic/non-basic flags
    basicVar.basic = false;
    nbVar.basic = true;
  }

  _varByIdx(idx) {
    for (const v of this.vars.values()) {
      if (v.index === idx) return v;
    }
    return null;
  }

  // Update a non-basic variable's value and adjust all basic variables
  _updateNonBasic(nbVar, newValue) {
    const delta = newValue - nbVar.value;
    if (Math.abs(delta) < 1e-15) return;
    nbVar.value = newValue;
    // Update all basic variables that depend on this non-basic variable
    for (const [bIdx, row] of this.rows) {
      if (row.has(nbVar.index)) {
        const bVar = this._varByIdx(bIdx);
        bVar.value += row.get(nbVar.index) * delta;
      }
    }
  }

  // Get model (values of original variables)
  getModel() {
    const model = {};
    for (const [name, v] of this.vars) {
      if (!name.startsWith('__slack_')) {
        model[name] = v.value;
      }
    }
    return model;
  }

  // Checkpoint for backtracking
  checkpoint() {
    // Save full variable state snapshot
    const snapshot = new Map();
    for (const [name, v] of this.vars) {
      snapshot.set(name, { lower: v.lower, upper: v.upper, value: v.value });
    }
    return { historyLen: this.boundHistory.length, snapshot };
  }

  backtrackTo(cp) {
    this.boundHistory.length = cp.historyLen;
    for (const [name, saved] of cp.snapshot) {
      const v = this.vars.get(name);
      if (v) {
        v.lower = saved.lower;
        v.upper = saved.upper;
        v.value = saved.value;
      }
    }
  }
}

module.exports = { Simplex };
