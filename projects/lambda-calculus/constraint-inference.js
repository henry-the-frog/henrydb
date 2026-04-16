/**
 * Constraint-Based Type Inference
 * 
 * Alternative to Algorithm W: separate constraint generation from solving.
 * 
 * Two phases:
 * 1. Generate: Walk AST, emit constraints like "type(e1) = type(e2) → α"
 * 2. Solve: Unify all constraints simultaneously
 * 
 * Advantage: cleaner separation, easier to extend with new constraint types.
 * Same result as Algorithm W for standard HM types.
 * 
 * Based on: Pierce, "Types and Programming Languages" Ch. 22
 */

import {
  TVar, TFun, TCon, tInt, tBool, tStr,
  Scheme, Subst,
  EVar, ELam, EApp, ELet, ELit,
  evar, elam, eapp, elet, eint, ebool, estr,
  unify, ftv, ftvEnv,
  generalize, instantiate,
  freshVar, resetFresh
} from './hindley-milner.js';

// ============================================================
// Constraints
// ============================================================

class CEq {
  // Type equality constraint: t1 = t2
  constructor(t1, t2, source) {
    this.tag = 'CEq';
    this.t1 = t1;
    this.t2 = t2;
    this.source = source; // For error reporting
  }
  toString() { return `${this.t1} = ${this.t2}  [${this.source}]`; }
}

// ============================================================
// Constraint Generator
// ============================================================

class ConstraintGenerator {
  constructor() {
    this.constraints = [];
  }

  /**
   * Generate constraints from an expression.
   * Returns the type of the expression.
   */
  generate(expr, env = new Map()) {
    switch (expr.tag) {
      case 'ELit':
        return expr.type;
      
      case 'EVar': {
        const scheme = env.get(expr.name);
        if (!scheme) throw new Error(`Unbound variable: ${expr.name}`);
        return instantiate(scheme);
      }
      
      case 'ELam': {
        const paramType = freshVar();
        const newEnv = new Map(env);
        newEnv.set(expr.param, new Scheme([], paramType));
        const bodyType = this.generate(expr.body, newEnv);
        return new TFun(paramType, bodyType);
      }
      
      case 'EApp': {
        const fnType = this.generate(expr.fn, env);
        const argType = this.generate(expr.arg, env);
        const retType = freshVar();
        // Constraint: fnType = argType → retType
        this.constraints.push(new CEq(fnType, new TFun(argType, retType), 'application'));
        return retType;
      }
      
      case 'ELet': {
        const valType = this.generate(expr.val, env);
        // For let-polymorphism: solve constraints so far, generalize
        const subst = this._solvePartial();
        const generalizedType = generalize(env, subst.apply(valType));
        const newEnv = new Map(env);
        newEnv.set(expr.name, generalizedType);
        return this.generate(expr.body, newEnv);
      }
      
      default:
        throw new Error(`Unknown expression: ${expr.tag}`);
    }
  }

  _solvePartial() {
    // Solve accumulated constraints
    let subst = new Subst();
    for (const c of this.constraints) {
      const s = unify(subst.apply(c.t1), subst.apply(c.t2));
      subst = s.compose(subst);
    }
    return subst;
  }

  /**
   * Get all generated constraints
   */
  getConstraints() {
    return this.constraints;
  }
}

// ============================================================
// Constraint Solver
// ============================================================

class ConstraintSolver {
  /**
   * Solve a set of equality constraints by unification
   */
  solve(constraints) {
    let subst = new Subst();
    const errors = [];
    
    for (const c of constraints) {
      try {
        const s = unify(subst.apply(c.t1), subst.apply(c.t2));
        subst = s.compose(subst);
      } catch (e) {
        errors.push({ constraint: c, error: e.message });
      }
    }
    
    return { subst, errors };
  }
}

// ============================================================
// Combined: infer via constraints
// ============================================================

function inferByConstraints(expr, env = new Map()) {
  resetFresh();
  const generator = new ConstraintGenerator();
  const exprType = generator.generate(expr, env);
  const constraints = generator.getConstraints();
  
  const solver = new ConstraintSolver();
  const { subst, errors } = solver.solve(constraints);
  
  return {
    type: subst.apply(exprType),
    constraints,
    subst,
    errors
  };
}

// ============================================================
// Exports
// ============================================================

export {
  CEq, ConstraintGenerator, ConstraintSolver, inferByConstraints
};
