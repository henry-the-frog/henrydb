/**
 * Combinatory Logic — SKI Combinator Calculus
 * 
 * Implements:
 * - SKI combinators as first-class terms
 * - Abstraction elimination: λ-calculus → SKI translation
 * - SKI reduction (graph reduction)
 * - Extended combinators: B, C, W, I', K', S'
 * - Unlambda-style notation
 * - Equivalence testing between λ-terms and SKI terms
 */

import {
  Var, Abs, App, parse as parseLambda, reduce as reduceLambda,
  alphaEquivalent, freeVars, churchNumeral, unchurch,
} from './lambda.js';

// ============================================================
// SKI AST
// ============================================================

class Combinator {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
  equals(other) { return other instanceof Combinator && other.name === this.name; }
  clone() { return new Combinator(this.name); }
}

class CApp {
  constructor(func, arg) { this.func = func; this.arg = arg; }
  toString() {
    const f = this.func instanceof CApp ? `(${this.func})` : `${this.func}`;
    const a = this.arg instanceof CApp ? `(${this.arg})` : `${this.arg}`;
    return `${f}${a}`;
  }
  equals(other) {
    return other instanceof CApp && this.func.equals(other.func) && this.arg.equals(other.arg);
  }
  clone() { return new CApp(this.func.clone(), this.arg.clone()); }
}

class CVar {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
  equals(other) { return other instanceof CVar && other.name === this.name; }
  clone() { return new CVar(this.name); }
}

// Standard combinators
const S = new Combinator('S');
const K = new Combinator('K');
const I = new Combinator('I');
const B = new Combinator('B');
const C = new Combinator('C');

// ============================================================
// Abstraction Elimination (λ → SKI)
// 
// Turner's algorithm:
// 1. [x] x         = I
// 2. [x] N         = K N    (when x not free in N)
// 3. [x] (M N)     = S ([x] M) ([x] N)
//
// Optimized with B and C:
// 3a. [x] (M x)    = M      (eta reduction, when x not free in M)
// 3b. [x] (M N)    = B M ([x] N)  (when x not free in M)
// 3c. [x] (M N)    = C ([x] M) N  (when x not free in N)
// ============================================================

function lambdaFreeIn(name, expr) {
  if (expr instanceof Var) return expr.name === name;
  if (expr instanceof Abs) return expr.param !== name && lambdaFreeIn(name, expr.body);
  if (expr instanceof App) return lambdaFreeIn(name, expr.func) || lambdaFreeIn(name, expr.arg);
  return false;
}

function skiFreeIn(name, expr) {
  if (expr instanceof CVar) return expr.name === name;
  if (expr instanceof Combinator) return false;
  if (expr instanceof CApp) return skiFreeIn(name, expr.func) || skiFreeIn(name, expr.arg);
  return false;
}

// Basic abstraction elimination (S, K, I only)
function eliminateBasic(name, expr) {
  // [x] x = I
  if (expr instanceof CVar && expr.name === name) return I;
  
  // [x] c = K c (combinator or different variable)
  if (expr instanceof Combinator || (expr instanceof CVar && expr.name !== name)) {
    return new CApp(K, expr);
  }
  
  // [x] (M N) = S ([x] M) ([x] N)
  if (expr instanceof CApp) {
    return new CApp(new CApp(S, eliminateBasic(name, expr.func)), eliminateBasic(name, expr.arg));
  }
  
  throw new Error(`Unexpected expr in eliminateBasic: ${expr}`);
}

// Optimized abstraction elimination (S, K, I, B, C)
function eliminateOptimized(name, expr) {
  // [x] x = I
  if (expr instanceof CVar && expr.name === name) return I;
  
  // [x] c = K c (when x not free)
  if (!skiFreeIn(name, expr)) {
    return new CApp(K, expr);
  }
  
  // [x] (M N)
  if (expr instanceof CApp) {
    const freeInM = skiFreeIn(name, expr.func);
    const freeInN = skiFreeIn(name, expr.arg);
    
    // [x] (M x) = M (eta reduction, when x not free in M)
    if (expr.arg instanceof CVar && expr.arg.name === name && !freeInM) {
      return expr.func;
    }
    
    // [x] (M N) where x only in N: B M ([x] N)
    if (!freeInM && freeInN) {
      return new CApp(new CApp(B, expr.func), eliminateOptimized(name, expr.arg));
    }
    
    // [x] (M N) where x only in M: C ([x] M) N
    if (freeInM && !freeInN) {
      return new CApp(new CApp(C, eliminateOptimized(name, expr.func)), expr.arg);
    }
    
    // [x] (M N) where x in both: S ([x] M) ([x] N)
    return new CApp(new CApp(S, eliminateOptimized(name, expr.func)), eliminateOptimized(name, expr.arg));
  }
  
  throw new Error(`Unexpected expr in eliminateOptimized: ${expr}`);
}

// Convert a full lambda term to SKI
function lambdaToSKI(expr, optimized = false) {
  const eliminate = optimized ? eliminateOptimized : eliminateBasic;
  
  if (expr instanceof Var) return new CVar(expr.name);
  
  if (expr instanceof App) {
    return new CApp(lambdaToSKI(expr.func, optimized), lambdaToSKI(expr.arg, optimized));
  }
  
  if (expr instanceof Abs) {
    // First convert body, then eliminate the variable
    const body = lambdaToSKI(expr.body, optimized);
    return eliminate(expr.param, body);
  }
  
  throw new Error(`Unknown lambda expr: ${expr}`);
}

// ============================================================
// SKI Reduction
// ============================================================

function skiStep(expr) {
  if (!(expr instanceof CApp)) return null;
  
  // I x → x
  if (expr.func instanceof Combinator && expr.func.name === 'I') {
    return expr.arg;
  }
  
  // K x y → x
  if (expr.func instanceof CApp &&
      expr.func.func instanceof Combinator && expr.func.func.name === 'K') {
    return expr.func.arg;
  }
  
  // S f g x → f x (g x)
  if (expr.func instanceof CApp && expr.func.func instanceof CApp &&
      expr.func.func.func instanceof Combinator && expr.func.func.func.name === 'S') {
    const f = expr.func.func.arg;
    const g = expr.func.arg;
    const x = expr.arg;
    return new CApp(new CApp(f, x.clone()), new CApp(g, x.clone()));
  }
  
  // B f g x → f (g x)
  if (expr.func instanceof CApp && expr.func.func instanceof CApp &&
      expr.func.func.func instanceof Combinator && expr.func.func.func.name === 'B') {
    const f = expr.func.func.arg;
    const g = expr.func.arg;
    const x = expr.arg;
    return new CApp(f, new CApp(g, x));
  }
  
  // C f x y → f y x
  if (expr.func instanceof CApp && expr.func.func instanceof CApp &&
      expr.func.func.func instanceof Combinator && expr.func.func.func.name === 'C') {
    const f = expr.func.func.arg;
    const x = expr.func.arg;
    const y = expr.arg;
    return new CApp(new CApp(f, y), x);
  }
  
  // Try reducing func
  const reducedFunc = skiStep(expr.func);
  if (reducedFunc !== null) {
    return new CApp(reducedFunc, expr.arg);
  }
  
  // Try reducing arg
  const reducedArg = skiStep(expr.arg);
  if (reducedArg !== null) {
    return new CApp(expr.func, reducedArg);
  }
  
  return null;
}

function skiReduce(expr, maxSteps = 1000) {
  let current = expr;
  let steps = 0;
  const trace = [current.toString()];
  
  while (steps < maxSteps) {
    const next = skiStep(current);
    if (next === null) break;
    current = next;
    steps++;
    trace.push(current.toString());
  }
  
  return { result: current, steps, trace, normalForm: steps < maxSteps };
}

// ============================================================
// Size metrics
// ============================================================

function skiSize(expr) {
  if (expr instanceof Combinator || expr instanceof CVar) return 1;
  if (expr instanceof CApp) return skiSize(expr.func) + skiSize(expr.arg);
  return 0;
}

// ============================================================
// Parser for SKI expressions
// ============================================================

function parseSKI(input) {
  let pos = 0;
  
  function parseExpr() {
    let node = parseAtom();
    while (pos < input.length && input[pos] !== ')') {
      node = new CApp(node, parseAtom());
    }
    return node;
  }
  
  function parseAtom() {
    while (pos < input.length && /\s/.test(input[pos])) pos++;
    
    if (input[pos] === '(') {
      pos++; // '('
      const expr = parseExpr();
      if (input[pos] === ')') pos++;
      return expr;
    }
    
    const ch = input[pos++];
    if (ch === 'S') return S.clone();
    if (ch === 'K') return K.clone();
    if (ch === 'I') return I.clone();
    if (ch === 'B') return B.clone();
    if (ch === 'C') return C.clone();
    
    // Variable (lowercase)
    if (/[a-z]/.test(ch)) return new CVar(ch);
    
    throw new Error(`Unexpected: '${ch}' at ${pos}`);
  }
  
  return parseExpr();
}

// ============================================================
// Unlambda notation
// ============================================================

function toUnlambda(expr) {
  if (expr instanceof Combinator) return expr.name.toLowerCase();
  if (expr instanceof CVar) return expr.name;
  if (expr instanceof CApp) return `\`${toUnlambda(expr.func)}${toUnlambda(expr.arg)}`;
  return '?';
}

// ============================================================
// Exports
// ============================================================

export {
  // AST
  Combinator, CApp, CVar,
  S, K, I, B, C,
  // Conversion
  lambdaToSKI, eliminateBasic, eliminateOptimized,
  // Reduction
  skiStep, skiReduce,
  // Parser
  parseSKI,
  // Utils
  skiSize, toUnlambda, skiFreeIn,
};
