/**
 * Normalization by Evaluation (NbE)
 * 
 * A modern technique for normalizing lambda terms that:
 * - Interprets terms into a semantic domain (JavaScript values)
 * - "Reads back" the semantic values into normal-form terms
 * - Produces beta-normal eta-long forms
 * - Is much faster than syntactic reduction for large terms
 * - Used in proof assistants (Lean, Agda, Coq)
 * 
 * Key idea: instead of reducing terms step-by-step,
 * evaluate them into host-language functions, then
 * "quote" the result back into syntax.
 */

import { Var, Abs, App, parse, alphaEquivalent, freeVars } from './lambda.js';

// ============================================================
// Semantic Domain
// 
// Values are either:
// - VNeutral: a stuck computation (variable applied to args)
// - VLam: a host-language closure
// ============================================================

class VNeutral {
  constructor(head, args = []) {
    this.head = head;  // variable name
    this.args = args;  // list of semantic values applied to it
  }
}

class VLam {
  constructor(param, closure) {
    this.param = param;
    this.closure = closure;  // JavaScript function: Value → Value
  }
}

// ============================================================
// Evaluation: Term → Value
// ============================================================

function evaluate(term, env = new Map()) {
  if (term instanceof Var) {
    if (env.has(term.name)) return env.get(term.name);
    return new VNeutral(term.name);
  }
  
  if (term instanceof Abs) {
    return new VLam(term.param, (argVal) => {
      const newEnv = new Map(env);
      newEnv.set(term.param, argVal);
      return evaluate(term.body, newEnv);
    });
  }
  
  if (term instanceof App) {
    const funcVal = evaluate(term.func, env);
    const argVal = evaluate(term.arg, env);
    return doApply(funcVal, argVal);
  }
  
  throw new Error(`Unknown term: ${term}`);
}

function doApply(funcVal, argVal) {
  if (funcVal instanceof VLam) {
    return funcVal.closure(argVal);
  }
  if (funcVal instanceof VNeutral) {
    return new VNeutral(funcVal.head, [...funcVal.args, argVal]);
  }
  throw new Error(`Cannot apply non-function value`);
}

// ============================================================
// Readback: Value → Term (quote back to normal form)
// ============================================================

let readbackCounter = 0;
function freshReadback() { return `_nbe${readbackCounter++}`; }
function resetReadback() { readbackCounter = 0; }

function readback(val) {
  if (val instanceof VNeutral) {
    let term = new Var(val.head);
    for (const arg of val.args) {
      term = new App(term, readback(arg));
    }
    return term;
  }
  
  if (val instanceof VLam) {
    const name = val.param || freshReadback();
    const argVal = new VNeutral(name);
    const bodyVal = val.closure(argVal);
    return new Abs(name, readback(bodyVal));
  }
  
  throw new Error(`Cannot readback: ${val}`);
}

// ============================================================
// Normalization: Term → Normal Form Term
// ============================================================

function normalize(term) {
  resetReadback();
  const val = evaluate(term);
  return readback(val);
}

// ============================================================
// Beta-Eta Equality Check
// Two terms are equal if they normalize to the same thing
// ============================================================

function betaEtaEqual(a, b) {
  const na = normalize(a);
  const nb = normalize(b);
  return alphaEquivalent(na, nb);
}

// ============================================================
// Exports
// ============================================================

export {
  VNeutral, VLam,
  evaluate, doApply,
  readback, normalize,
  betaEtaEqual,
  resetReadback,
};
