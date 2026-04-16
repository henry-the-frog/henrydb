/**
 * Totality Checking
 * 
 * Three aspects of totality:
 * 1. Coverage: all pattern cases handled (no missing cases)
 * 2. Termination: recursive calls decrease some measure
 * 3. Productivity: corecursive definitions produce output
 * 
 * Total functions are guaranteed to terminate on all inputs.
 * Required for proofs (Agda, Coq) but optional elsewhere.
 */

// ============================================================
// Coverage checking (exhaustiveness)
// ============================================================

class Pattern {
  static wildcard() { return { tag: 'PWild' }; }
  static con(name, args = []) { return { tag: 'PCon', name, args }; }
  static lit(value) { return { tag: 'PLit', value }; }
  static var(name) { return { tag: 'PVar', name }; }
}

function checkCoverage(datatype, patterns) {
  const constructors = new Set(datatype.constructors.map(c => c.name));
  const coveredConstructors = new Set();
  let hasWildcard = false;

  for (const pat of patterns) {
    if (pat.tag === 'PWild' || pat.tag === 'PVar') {
      hasWildcard = true;
      break;
    }
    if (pat.tag === 'PCon') coveredConstructors.add(pat.name);
  }

  if (hasWildcard) return { exhaustive: true, missing: [] };

  const missing = [...constructors].filter(c => !coveredConstructors.has(c));
  return { exhaustive: missing.length === 0, missing };
}

// ============================================================
// Termination checking (structural recursion)
// ============================================================

class CallGraph {
  constructor() { this.calls = []; }

  addCall(caller, callee, args) {
    this.calls.push({ caller, callee, args });
  }

  /**
   * Check if all recursive calls decrease on some argument
   */
  checkTermination(fnName) {
    const recursiveCalls = this.calls.filter(c => c.caller === fnName && c.callee === fnName);

    if (recursiveCalls.length === 0) return { terminates: true, reason: 'no recursion' };

    for (let argIdx = 0; argIdx < (recursiveCalls[0]?.args.length || 0); argIdx++) {
      const allDecrease = recursiveCalls.every(call => {
        const argInfo = call.args[argIdx];
        return argInfo && argInfo.decreasing;
      });
      if (allDecrease) return { terminates: true, reason: `structural decrease on arg ${argIdx}` };
    }

    return { terminates: false, reason: 'no decreasing argument found' };
  }
}

/**
 * Check if an expression is structurally smaller
 */
function isStructurallySmaller(expr, param) {
  // Direct subterms of the parameter are smaller
  if (expr.tag === 'Decon' && expr.of === param) return true;
  // Recursive calls on subterms
  if (expr.tag === 'Var' && expr.subOf === param) return true;
  return false;
}

// ============================================================
// Productivity checking (for corecursive definitions)
// ============================================================

function checkProductivity(def) {
  // A corecursive definition is productive if every path through
  // the body produces at least one constructor before recursing
  
  if (def.guardedBy) {
    // Definition is guarded by a constructor (e.g., Cons(x, recurse))
    return { productive: true, reason: `guarded by ${def.guardedBy}` };
  }
  
  return { productive: false, reason: 'unguarded recursion' };
}

// ============================================================
// Simple termination metric
// ============================================================

function checkMetric(fn, inputs, metric) {
  const results = [];
  for (const input of inputs) {
    const m = metric(input);
    results.push({ input, metric: m, valid: m >= 0 });
  }
  return {
    allValid: results.every(r => r.valid),
    results
  };
}

export {
  Pattern, checkCoverage,
  CallGraph, isStructurallySmaller,
  checkProductivity, checkMetric
};
