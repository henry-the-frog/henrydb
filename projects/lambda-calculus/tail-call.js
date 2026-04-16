/**
 * Tail Call Analysis
 * 
 * Detects which function calls are in tail position and can be optimized
 * to jumps (eliminating stack growth).
 * 
 * A call is in tail position if it is the last thing the function does.
 * 
 * Tail positions:
 * - Body of a function
 * - Then/else branches of if in tail position
 * - Body of let in tail position
 * - NOT: function position of application
 * - NOT: argument position of application
 * - NOT: operand of primitive operation
 */

// ============================================================
// Expressions
// ============================================================

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Lam { constructor(name, param, body) { this.tag = 'Lam'; this.name = name; this.param = param; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Prim { constructor(op, l, r) { this.tag = 'Prim'; this.op = op; this.l = l; this.r = r; } }
class If { constructor(cond, then, els) { this.tag = 'If'; this.cond = cond; this.then = then; this.els = els; } }
class Let { constructor(name, val, body) { this.tag = 'Let'; this.name = name; this.val = val; this.body = body; } }

// ============================================================
// Tail Call Analysis Result
// ============================================================

class TailCallInfo {
  constructor() {
    this.tailCalls = [];      // [{expr, callee, isSelfRecursive}]
    this.nonTailCalls = [];   // [{expr, callee}]
    this.currentFunction = null;
  }
}

// ============================================================
// Analyzer
// ============================================================

class TailCallAnalyzer {
  constructor() {
    this.results = [];
    this.currentFunction = null;
  }

  /**
   * Analyze an expression for tail calls
   */
  analyze(expr) {
    this.results = [];
    this._analyze(expr, true); // Start in tail position
    return {
      tailCalls: this.results.filter(r => r.isTail),
      nonTailCalls: this.results.filter(r => !r.isTail),
      selfTailCalls: this.results.filter(r => r.isTail && r.isSelfRecursive),
      total: this.results.length
    };
  }

  _analyze(expr, isTail) {
    switch (expr.tag) {
      case 'Num': case 'Var': return;
      
      case 'Lam': {
        const savedFn = this.currentFunction;
        this.currentFunction = expr.name;
        this._analyze(expr.body, true); // Body is in tail position
        this.currentFunction = savedFn;
        return;
      }
      
      case 'App': {
        // The APPLICATION is in tail position (or not)
        const calleeName = expr.fn.tag === 'Var' ? expr.fn.name : null;
        this.results.push({
          expr,
          callee: calleeName,
          isTail,
          isSelfRecursive: calleeName === this.currentFunction
        });
        
        // Function and argument are NOT in tail position
        this._analyze(expr.fn, false);
        this._analyze(expr.arg, false);
        return;
      }
      
      case 'Prim': {
        // Operands are never in tail position
        this._analyze(expr.l, false);
        this._analyze(expr.r, false);
        return;
      }
      
      case 'If': {
        // Condition is not in tail position
        this._analyze(expr.cond, false);
        // Both branches inherit tail position
        this._analyze(expr.then, isTail);
        this._analyze(expr.els, isTail);
        return;
      }
      
      case 'Let': {
        // Value is not in tail position
        this._analyze(expr.val, false);
        // Body inherits tail position
        this._analyze(expr.body, isTail);
        return;
      }
    }
  }
}

/**
 * Check if an expression can be tail-call optimized (all recursive calls are tail calls)
 */
function canTCO(funcName, body) {
  const analyzer = new TailCallAnalyzer();
  analyzer.currentFunction = funcName;
  analyzer._analyze(body, true);
  
  const selfCalls = analyzer.results.filter(r => r.callee === funcName);
  const nonTailSelf = selfCalls.filter(r => !r.isTail);
  
  return {
    canOptimize: nonTailSelf.length === 0 && selfCalls.length > 0,
    selfCalls: selfCalls.length,
    tailSelfCalls: selfCalls.filter(r => r.isTail).length,
    nonTailSelfCalls: nonTailSelf.length
  };
}

export {
  Var, Lam, App, Num, Prim, If, Let,
  TailCallAnalyzer, canTCO
};
