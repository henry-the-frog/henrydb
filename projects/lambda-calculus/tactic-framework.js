/**
 * 🎉🎉🎉🎉🎉🎉🎉 MODULE #140: Tactic Framework 🎉🎉🎉🎉🎉🎉🎉
 * 
 * A composable tactic language for proof construction.
 * Tactics transform goals into subgoals until all are solved.
 * 
 * Core tactics: intro, apply, exact, split, left, right, assumption
 * Combinators: then (;), orelse (|), try, repeat
 */

class Goal {
  constructor(hyps, target) { this.hyps = hyps; this.target = target; }
  toString() { return `${[...this.hyps].map(([k,v]) => `${k}:${v}`).join(', ')} ⊢ ${this.target}`; }
}

class ProofState {
  constructor(goals, solved = []) { this.goals = goals; this.solved = solved; }
  get done() { return this.goals.length === 0; }
  get current() { return this.goals[0]; }
}

// Core tactics
function intro(name) {
  return (state) => {
    const goal = state.current;
    if (!goal.target.includes('→')) throw new Error('intro: not an implication');
    const [param, ...rest] = goal.target.split(' → ');
    const newTarget = rest.join(' → ');
    const newHyps = new Map([...goal.hyps, [name, param.trim()]]);
    return new ProofState([new Goal(newHyps, newTarget), ...state.goals.slice(1)], state.solved);
  };
}

function exact(name) {
  return (state) => {
    const goal = state.current;
    const hypType = goal.hyps.get(name);
    if (!hypType || hypType !== goal.target) throw new Error(`exact: ${name} has type ${hypType}, need ${goal.target}`);
    return new ProofState(state.goals.slice(1), [...state.solved, goal]);
  };
}

function assumption(state) {
  const goal = state.current;
  for (const [name, type] of goal.hyps) {
    if (type === goal.target) return new ProofState(state.goals.slice(1), [...state.solved, goal]);
  }
  throw new Error('assumption: no matching hypothesis');
}

function split(state) {
  const goal = state.current;
  if (!goal.target.includes(' ∧ ')) throw new Error('split: not a conjunction');
  const [left, right] = goal.target.split(' ∧ ');
  return new ProofState([new Goal(goal.hyps, left.trim()), new Goal(goal.hyps, right.trim()), ...state.goals.slice(1)], state.solved);
}

function left(state) {
  const goal = state.current;
  if (!goal.target.includes(' ∨ ')) throw new Error('left: not a disjunction');
  const [l] = goal.target.split(' ∨ ');
  return new ProofState([new Goal(goal.hyps, l.trim()), ...state.goals.slice(1)], state.solved);
}

function right(state) {
  const goal = state.current;
  if (!goal.target.includes(' ∨ ')) throw new Error('right: not a disjunction');
  const parts = goal.target.split(' ∨ ');
  return new ProofState([new Goal(goal.hyps, parts.slice(1).join(' ∨ ').trim()), ...state.goals.slice(1)], state.solved);
}

// Combinators
function then(...tactics) {
  return (state) => tactics.reduce((s, t) => t(s), state);
}

function orelse(t1, t2) {
  return (state) => { try { return t1(state); } catch { return t2(state); } };
}

function tryTactic(t) {
  return orelse(t, state => state);
}

function repeatTactic(t, max = 10) {
  return (state) => {
    let current = state;
    for (let i = 0; i < max; i++) {
      try { const next = t(current); if (next === current) break; current = next; }
      catch { break; }
    }
    return current;
  };
}

// Proof runner
function prove(target, tactics) {
  let state = new ProofState([new Goal(new Map(), target)]);
  for (const tactic of tactics) state = tactic(state);
  return state;
}

export { Goal, ProofState, intro, exact, assumption, split, left, right, then, orelse, tryTactic, repeatTactic, prove };
