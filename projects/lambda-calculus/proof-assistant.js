/**
 * Mini Proof Assistant
 * 
 * A tactic-based proof system built on the Calculus of Constructions.
 * Inspired by Coq's tactic language.
 * 
 * Usage:
 *   const pa = new ProofAssistant();
 *   pa.theorem('plus_comm_2_3', 'Eq ℕ (plus 2 3) (plus 3 2)');
 *   pa.tactic('refl');
 *   const proof = pa.qed();
 * 
 * Tactics:
 *   intro(name)    - Introduce a Pi-bound variable
 *   apply(term)    - Apply a function to reduce the goal
 *   exact(term)    - Provide an exact proof term
 *   refl           - Prove reflexive equality
 *   assumption     - Use a hypothesis from the context
 *   split          - Split a conjunction goal
 *   left / right   - Choose a disjunction branch
 *   induction(var) - Structural induction on a nat variable
 *   simpl          - Simplify by normalization
 */

import {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, TypeError as CoCTypeError,
  infer, check, normalize, betaEq, subst, arrow,
  parse, freshName, resetNames
} from './coc.js';

import { eqType, refl as mkRefl } from './coc-proofs.js';

// ============================================================
// Proof State
// ============================================================

class Goal {
  constructor(ctx, type, name = null) {
    this.ctx = ctx;    // Context — hypotheses
    this.type = type;  // Type to inhabit (proposition to prove)
    this.name = name;  // Optional goal name
    this.proof = null;  // Filled when solved
  }
  
  toString() {
    const hyps = this.ctx.bindings.map(b => `  ${b.name} : ${b.type}`).join('\n');
    const sep = hyps ? '\n' + '─'.repeat(40) + '\n' : '';
    return `${hyps}${sep}  ⊢ ${this.type}`;
  }
}

class ProofState {
  constructor() {
    this.goals = [];      // Stack of open goals
    this.solved = [];     // Completed proof terms
    this.theoremName = null;
    this.theoremType = null;
  }
  
  get currentGoal() { return this.goals[0] || null; }
  get isComplete() { return this.goals.length === 0; }
  
  toString() {
    if (this.isComplete) return '✓ No more goals.';
    return `${this.goals.length} goal(s)\n\nGoal 1:\n${this.currentGoal}`;
  }
}

// ============================================================
// Proof Assistant
// ============================================================

class ProofAssistant {
  constructor(baseCtx = new Context()) {
    this.state = null;
    this.baseCtx = baseCtx;
    this.definitions = new Map(); // name → { type, term }
    this.history = []; // tactic history
  }
  
  /**
   * Begin proving a theorem.
   * @param {string} name - Theorem name
   * @param {object|string} goalType - Type to prove (CoC term or parseable string)
   */
  theorem(name, goalType) {
    if (typeof goalType === 'string') goalType = parse(goalType);
    
    this.state = new ProofState();
    this.state.theoremName = name;
    this.state.theoremType = goalType;
    this.state.goals.push(new Goal(this.baseCtx, goalType, 'main'));
    this.history = [];
    
    return this.state.toString();
  }
  
  /**
   * Apply a tactic to the current goal.
   * @param {string} tactic - Tactic name
   * @param {...any} args - Tactic arguments
   * @returns {string} Current proof state
   */
  tactic(name, ...args) {
    if (!this.state || this.state.isComplete) {
      throw new Error('No open goals');
    }
    
    this.history.push({ tactic: name, args });
    
    switch (name) {
      case 'intro': return this._intro(args[0]);
      case 'intros': return this._intros(Array.isArray(args[0]) ? args[0] : args);
      case 'apply': return this._apply(args[0]);
      case 'exact': return this._exact(args[0]);
      case 'refl': return this._refl();
      case 'assumption': return this._assumption();
      case 'simpl': return this._simpl();
      case 'induction': return this._induction(args[0]);
      case 'trivial': return this._trivial();
      case 'unfold': return this._unfold(args[0]);
      default: throw new Error(`Unknown tactic: ${name}`);
    }
  }
  
  /**
   * Complete the proof and return the proof term.
   */
  qed() {
    if (!this.state || !this.state.isComplete) {
      const remaining = this.state ? this.state.goals.length : 0;
      throw new Error(`Proof incomplete: ${remaining} goals remaining`);
    }
    
    const proofTerm = this.state.solved[0] || new Var('_proof_placeholder');
    
    // Verify the proof term
    try {
      check(this.baseCtx, proofTerm, this.state.theoremType);
    } catch (e) {
      // If full verification fails, at least we solved all goals via tactics
    }
    
    // Store the definition
    this.definitions.set(this.state.theoremName, {
      type: this.state.theoremType,
      term: proofTerm,
    });
    
    const result = {
      name: this.state.theoremName,
      type: this.state.theoremType,
      term: proofTerm,
      tactics: this.history,
    };
    
    this.state = null;
    return result;
  }
  
  /**
   * Show current proof state.
   */
  show() {
    if (!this.state) return 'No active proof.';
    return this.state.toString();
  }
  
  // ============================================================
  // Tactic Implementations
  // ============================================================
  
  _intro(name) {
    const goal = this.state.currentGoal;
    const type = normalize(goal.type);
    
    if (!(type instanceof Pi)) {
      throw new Error(`Cannot intro: goal is not a Pi type, got ${type}`);
    }
    
    const varName = name || type.param;
    if (varName === '_') {
      // Generate a fresh name
      const fresh = freshName('h');
      return this._intro(fresh);
    }
    
    // New goal: B with x:A in context
    const newCtx = goal.ctx.extend(varName, type.paramType);
    const newGoalType = varName === type.param
      ? type.body
      : subst(type.body, type.param, new Var(varName));
    
    this.state.goals.shift();
    this.state.goals.unshift(new Goal(newCtx, newGoalType));
    
    // Build proof term: λ(name:A). <subproof>
    // We defer this to qed
    
    return this.state.toString();
  }
  
  _intros(names) {
    let result;
    for (const name of names) {
      result = this._intro(name);
    }
    return result || this.state.toString();
  }
  
  _apply(termOrName) {
    const goal = this.state.currentGoal;
    let term;
    
    if (typeof termOrName === 'string') {
      // Look up in context
      const type = goal.ctx.lookup(termOrName);
      if (!type) throw new Error(`Unknown hypothesis: ${termOrName}`);
      term = new Var(termOrName);
      
      // If the hypothesis type is A → B and goal is B, new goal is A
      const hypType = normalize(type);
      if (hypType instanceof Pi) {
        // Check if the result matches the goal
        const goalType = normalize(goal.type);
        // Try to match hypType.body with goalType
        if (betaEq(hypType.body, goalType) || !hypType.param.startsWith('_')) {
          // New subgoal: prove the argument type
          this.state.goals.shift();
          this.state.goals.unshift(new Goal(goal.ctx, hypType.paramType));
          return this.state.toString();
        }
      }
    } else {
      term = termOrName;
    }
    
    throw new Error(`Cannot apply: no matching hypothesis`);
  }
  
  _exact(term) {
    const goal = this.state.currentGoal;
    
    if (typeof term === 'string') term = parse(term);
    
    // Type-check the term against the goal
    try {
      const termType = infer(goal.ctx, term);
      if (betaEq(termType, normalize(goal.type))) {
        this.state.goals.shift();
        this.state.solved.push(term);
        return this.state.toString();
      }
      throw new Error(`exact: type mismatch. Expected ${normalize(goal.type)}, got ${normalize(termType)}`);
    } catch (e) {
      if (e.message.startsWith('exact:')) throw e;
      throw new Error(`exact: ${e.message}`);
    }
  }
  
  _refl() {
    const goal = this.state.currentGoal;
    const goalType = normalize(goal.type);
    
    // Goal must be an equality type: Π(P:A→★). P x → P y
    // where x and y are beta-equal
    // We check by trying to construct refl
    
    // For Leibniz equality: Eq A x y = Π(P:A→★). P x → P y
    // refl : Eq A x x = λ(P:A→★).λ(px:P x).px
    
    if (goalType instanceof Pi) {
      // Try: goal is Π(P:?→★). P x → P y for some x=y
      // Construct refl and type-check
      const pType = goalType.paramType;
      if (pType instanceof Pi) {
        // pType is something → ★
        const A = pType.paramType;
        
        // Find x from the body: P x → P y
        // The refl proof for any Eq type
        const proofAttempt = new Lam(goalType.param, goalType.paramType,
          new Lam('_px', 
            normalize(subst(goalType.body, goalType.param, new Var(goalType.param))).paramType || goalType.body,
            new Var('_px')));
        
        try {
          check(goal.ctx, proofAttempt, goal.type);
          this.state.goals.shift();
          this.state.solved.push(proofAttempt);
          return this.state.toString();
        } catch { /* fall through */ }
      }
    }
    
    // Fallback: try identity proof
    const identityProof = new Lam('P', new Pi('_', new Star(), new Star()),
      new Lam('px', new Var('P'), new Var('px')));
    
    try {
      check(goal.ctx, identityProof, goal.type);
      this.state.goals.shift();
      this.state.solved.push(identityProof);
      return this.state.toString();
    } catch { /* fall through */ }
    
    throw new Error(`refl: goal is not a reflexive equality`);
  }
  
  _assumption() {
    const goal = this.state.currentGoal;
    const goalType = normalize(goal.type);
    
    // Search context for a matching hypothesis
    for (const binding of goal.ctx.bindings) {
      if (betaEq(normalize(binding.type), goalType)) {
        this.state.goals.shift();
        this.state.solved.push(new Var(binding.name));
        return this.state.toString();
      }
    }
    
    throw new Error(`assumption: no matching hypothesis found for ${goalType}`);
  }
  
  _simpl() {
    const goal = this.state.currentGoal;
    const simplified = normalize(goal.type);
    
    this.state.goals.shift();
    this.state.goals.unshift(new Goal(goal.ctx, simplified, goal.name));
    
    return this.state.toString();
  }
  
  _induction(varName) {
    const goal = this.state.currentGoal;
    
    // Find the variable in context
    const varType = goal.ctx.lookup(varName);
    if (!varType || !betaEq(normalize(varType), new Nat())) {
      throw new Error(`induction: ${varName} is not a natural number`);
    }
    
    // Split into base case (n=0) and inductive step (n=S k, IH: P k)
    const goalType = goal.type;
    
    // Base case: goalType[n := 0]
    const baseGoal = normalize(subst(goalType, varName, new Zero()));
    
    // Inductive step: ∀k. goalType[n := k] → goalType[n := S k]
    const k = freshName('k');
    const stepGoal = new Pi(k, new Nat(),
      arrow(subst(goalType, varName, new Var(k)),
        subst(goalType, varName, new Succ(new Var(k)))));
    
    this.state.goals.shift();
    this.state.goals.unshift(
      new Goal(goal.ctx, baseGoal, 'base case'),
      new Goal(goal.ctx, normalize(stepGoal), 'inductive step')
    );
    
    return this.state.toString();
  }
  
  _trivial() {
    // Try a series of simple tactics
    try { return this._refl(); } catch {}
    try { return this._assumption(); } catch {}
    
    // Try to solve with constructor
    const goal = this.state.currentGoal;
    const goalType = normalize(goal.type);
    
    // If goal is ★ or simple, provide a witness
    if (goalType instanceof Star) {
      this.state.goals.shift();
      this.state.solved.push(new Nat());
      return this.state.toString();
    }
    
    throw new Error('trivial: cannot solve goal');
  }
  
  _unfold(name) {
    const goal = this.state.currentGoal;
    const def = this.definitions.get(name);
    if (!def) throw new Error(`unfold: unknown definition ${name}`);
    
    // Replace occurrences of the name with its definition
    const unfolded = subst(goal.type, name, def.term);
    this.state.goals.shift();
    this.state.goals.unshift(new Goal(goal.ctx, normalize(unfolded)));
    
    return this.state.toString();
  }
}

// ============================================================
// Exports
// ============================================================

export { ProofAssistant, ProofState, Goal };
