/**
 * Small-Step Operational Semantics Tracer
 * 
 * Shows each reduction step on the path to normal form.
 * Supports multiple reduction strategies.
 * 
 * Features:
 * - Step-by-step traces with redex highlighting
 * - Multiple strategies: normal order, applicative order, call-by-value, call-by-name
 * - Step counting and divergence detection
 * - Trace formatting for display
 */

import { 
  Var, Abs, App, 
  normalOrderStep, applicativeOrderStep, callByValueStep, callByNameStep,
  parse, prettyPrint, isValue
} from './lambda.js';

// ============================================================
// Trace Step
// ============================================================

class TraceStep {
  constructor(expr, rule, redex = null) {
    this.expr = expr;       // The expression at this step
    this.rule = rule;       // Which rule was applied (β-reduce, etc.)
    this.redex = redex;     // The specific redex that was reduced (optional)
  }

  toString() {
    const exprStr = prettyPrint(this.expr);
    return this.rule ? `${exprStr}  [${this.rule}]` : exprStr;
  }
}

// ============================================================
// Tracer
// ============================================================

class Tracer {
  constructor(options = {}) {
    this.maxSteps = options.maxSteps || 100;
    this.strategy = options.strategy || 'normal'; // normal, applicative, cbv, cbn
  }

  /**
   * Trace reduction to normal form (or divergence detection)
   */
  trace(expr) {
    const steps = [new TraceStep(expr, null)];
    let current = expr;
    const stepFn = this._getStepFunction();

    for (let i = 0; i < this.maxSteps; i++) {
      const rule = this._identifyRedex(current);
      const next = stepFn(current);
      if (next === null) {
        // Normal form reached
        return {
          steps,
          normalForm: current,
          stepCount: i,
          diverged: false
        };
      }
      steps.push(new TraceStep(next, rule));
      current = next;
    }

    // Divergence detected
    return {
      steps,
      normalForm: null,
      stepCount: this.maxSteps,
      diverged: true
    };
  }

  /**
   * Trace from source string
   */
  traceSource(source) {
    const expr = parse(source);
    return this.trace(expr);
  }

  /**
   * Count steps to normal form without storing trace
   */
  countSteps(expr) {
    let current = expr;
    const stepFn = this._getStepFunction();
    let count = 0;
    
    while (count < this.maxSteps) {
      const next = stepFn(current);
      if (next === null) return count;
      current = next;
      count++;
    }
    return -1; // Diverges
  }

  /**
   * Format trace as string array
   */
  formatTrace(trace) {
    return trace.steps.map((step, i) => {
      const prefix = i === 0 ? '  ' : '→ ';
      return `${prefix}${step}`;
    });
  }

  _getStepFunction() {
    switch (this.strategy) {
      case 'normal': return normalOrderStep;
      case 'applicative': return applicativeOrderStep;
      case 'cbv': return callByValueStep;
      case 'cbn': return callByNameStep;
      default: return normalOrderStep;
    }
  }

  _identifyRedex(expr) {
    if (expr instanceof App && expr.func instanceof Abs) {
      return 'β';
    }
    if (expr instanceof App) {
      if (this._hasRedex(expr.func)) return 'β (in func)';
      if (this._hasRedex(expr.arg)) return 'β (in arg)';
    }
    if (expr instanceof Abs) {
      if (this._hasRedex(expr.body)) return 'β (under λ)';
    }
    return 'β';
  }

  _hasRedex(expr) {
    if (expr instanceof App && expr.func instanceof Abs) return true;
    if (expr instanceof App) return this._hasRedex(expr.func) || this._hasRedex(expr.arg);
    if (expr instanceof Abs) return this._hasRedex(expr.body);
    return false;
  }
}

// ============================================================
// Strategy comparison
// ============================================================

/**
 * Compare how different strategies reduce the same expression
 */
function compareStrategies(expr) {
  const strategies = ['normal', 'applicative', 'cbv', 'cbn'];
  const results = {};
  
  for (const s of strategies) {
    const tracer = new Tracer({ strategy: s, maxSteps: 200 });
    const trace = tracer.trace(expr);
    results[s] = {
      steps: trace.stepCount,
      diverged: trace.diverged,
      normalForm: trace.normalForm ? prettyPrint(trace.normalForm) : null
    };
  }
  
  return results;
}

// ============================================================
// Church numeral utils for testing
// ============================================================

function churchNum(n) {
  // λf.λx. f(f(...f(x)...))
  let body = new Var('x');
  for (let i = 0; i < n; i++) body = new App(new Var('f'), body);
  return new Abs('f', new Abs('x', body));
}

function isChurchNum(expr) {
  if (!(expr instanceof Abs) || !(expr.body instanceof Abs)) return false;
  const f = expr.param;
  const x = expr.body.param;
  let body = expr.body.body;
  let count = 0;
  while (body instanceof App) {
    if (!(body.func instanceof Var) || body.func.name !== f) return false;
    body = body.arg;
    count++;
  }
  if (!(body instanceof Var) || body.name !== x) return false;
  return count;
}

// ============================================================
// Exports
// ============================================================

export { 
  TraceStep, Tracer, compareStrategies, 
  churchNum, isChurchNum 
};
