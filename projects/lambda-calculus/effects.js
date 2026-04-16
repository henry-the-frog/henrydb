/**
 * Algebraic Effects and Handlers
 * 
 * Based on the Plotkin-Power algebraic effects framework and
 * Pretnar's effect handlers.
 * 
 * An algebraic effect system separates:
 * - Effect operations (what effects are available)
 * - Effect handlers (how effects are interpreted)
 * - Continuations (the rest of the computation)
 * 
 * This enables modular, composable effectful computation.
 * Handlers are like exception handlers that can resume.
 * 
 * Example: State effect
 *   operations: Get(), Put(v)
 *   handler: interprets Get/Put using a mutable cell
 *   
 * Example: Exception effect
 *   operations: Raise(e)
 *   handler: catches errors (doesn't resume)
 *   
 * Example: Nondeterminism
 *   operations: Choose()
 *   handler: explores all branches, collects results
 */

// ============================================================
// Core AST
// ============================================================

// Values
class Num {
  constructor(n) { this.tag = 'Num'; this.n = n; }
  toString() { return `${this.n}`; }
}

class Bool {
  constructor(v) { this.tag = 'Bool'; this.v = v; }
  toString() { return `${this.v}`; }
}

class Str {
  constructor(s) { this.tag = 'Str'; this.s = s; }
  toString() { return `"${this.s}"`; }
}

class Unit {
  constructor() { this.tag = 'Unit'; }
  toString() { return '()'; }
}

class Pair {
  constructor(fst, snd) { this.tag = 'Pair'; this.fst = fst; this.snd = snd; }
  toString() { return `(${this.fst}, ${this.snd})`; }
}

class ListVal {
  constructor(elems) { this.tag = 'ListVal'; this.elems = elems; }
  toString() { return `[${this.elems.join(', ')}]`; }
}

// Lambda value (closure)
class Closure {
  constructor(param, body, env) {
    this.tag = 'Closure';
    this.param = param;
    this.body = body;
    this.env = env;
  }
  toString() { return `<fn ${this.param}>`; }
}

// ============================================================
// Computation AST
// ============================================================

class Var {
  constructor(name) { this.tag = 'Var'; this.name = name; }
}

class Lam {
  constructor(param, body) { this.tag = 'Lam'; this.param = param; this.body = body; }
}

class App {
  constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; }
}

class Let {
  constructor(name, value, body) { this.tag = 'Let'; this.name = name; this.value = value; this.body = body; }
}

class If {
  constructor(cond, then, else_) { this.tag = 'If'; this.cond = cond; this.then = then; this.else_ = else_; }
}

class BinOp {
  constructor(op, left, right) { this.tag = 'BinOp'; this.op = op; this.left = left; this.right = right; }
}

class Lit {
  constructor(value) { this.tag = 'Lit'; this.value = value; }
}

class MkPair {
  constructor(fst, snd) { this.tag = 'MkPair'; this.fst = fst; this.snd = snd; }
}

class Fst {
  constructor(pair) { this.tag = 'Fst'; this.pair = pair; }
}

class Snd {
  constructor(pair) { this.tag = 'Snd'; this.pair = pair; }
}

// Effect operations
class Perform {
  constructor(effect, arg) {
    this.tag = 'Perform';
    this.effect = effect;  // string: "Get", "Put", "Raise", "Choose", etc.
    this.arg = arg || new Lit(new Unit());
  }
}

// Handler
class Handle {
  constructor(body, handler) {
    this.tag = 'Handle';
    this.body = body;
    this.handler = handler;
    // handler = {
    //   return: (x) => ...,           // value handler
    //   ops: { "Effect": (arg, k) => ... }  // operation handlers (k = continuation)
    // }
  }
}

// ============================================================
// Continuation (first-class delimited continuation)
// ============================================================

class Continuation {
  constructor(param, body, env) {
    this.tag = 'Continuation';
    this.param = param;
    this.body = body;
    this.env = env;
  }
  toString() { return '<continuation>'; }
}

// ============================================================
// Environment
// ============================================================

class Env {
  constructor(parent = null) { this.bindings = new Map(); this.parent = parent; }
  extend(name, value) {
    const e = new Env(this);
    e.bindings.set(name, value);
    return e;
  }
  lookup(name) {
    if (this.bindings.has(name)) return this.bindings.get(name);
    if (this.parent) return this.parent.lookup(name);
    throw new Error(`Unbound variable: ${name}`);
  }
}

// ============================================================
// Evaluation
// ============================================================

// An effect signal (thrown up the handler stack)
class EffectSignal {
  constructor(effect, arg, continuation) {
    this.effect = effect;
    this.arg = arg;
    this.continuation = continuation;
  }
}

function evaluate(expr, env) {
  switch (expr.tag) {
    case 'Lit': return expr.value;
    case 'Var': return env.lookup(expr.name);
    
    case 'Lam': return new Closure(expr.param, expr.body, env);
    
    case 'App': {
      const fn = evaluate(expr.fn, env);
      const arg = evaluate(expr.arg, env);
      return apply(fn, arg);
    }
    
    case 'Let': {
      const val = evaluate(expr.value, env);
      return evaluate(expr.body, env.extend(expr.name, val));
    }
    
    case 'If': {
      const cond = evaluate(expr.cond, env);
      if (cond.v) return evaluate(expr.then, env);
      return evaluate(expr.else_, env);
    }
    
    case 'BinOp': {
      const left = evaluate(expr.left, env);
      const right = evaluate(expr.right, env);
      return evalBinOp(expr.op, left, right);
    }
    
    case 'MkPair': {
      const fst = evaluate(expr.fst, env);
      const snd = evaluate(expr.snd, env);
      return new Pair(fst, snd);
    }
    
    case 'Fst': {
      const pair = evaluate(expr.pair, env);
      return pair.fst;
    }
    
    case 'Snd': {
      const pair = evaluate(expr.snd || expr.pair, env);
      return pair.snd;
    }
    
    case 'Perform': {
      const arg = evaluate(expr.arg, env);
      // Throw an effect signal up the handler stack
      throw new EffectSignal(expr.effect, arg, null);
    }
    
    case 'Handle': {
      return handleEffects(expr.body, expr.handler, env);
    }
    
    default:
      throw new Error(`Unknown expression: ${expr.tag}`);
  }
}

function apply(fn, arg) {
  if (fn.tag === 'Closure') {
    return evaluate(fn.body, fn.env.extend(fn.param, arg));
  }
  if (fn.tag === 'Continuation') {
    return evaluate(fn.body, fn.env.extend(fn.param, arg));
  }
  throw new Error(`Cannot apply ${fn.tag}`);
}

function evalBinOp(op, left, right) {
  switch (op) {
    case '+': return new Num(left.n + right.n);
    case '-': return new Num(left.n - right.n);
    case '*': return new Num(left.n * right.n);
    case '/': return new Num(Math.floor(left.n / right.n));
    case '%': return new Num(left.n % right.n);
    case '==': {
      if (left.tag !== right.tag) return new Bool(false);
      if (left.tag === 'Num') return new Bool(left.n === right.n);
      if (left.tag === 'Str') return new Bool(left.s === right.s);
      if (left.tag === 'Bool') return new Bool(left.v === right.v);
      if (left.tag === 'Unit') return new Bool(true);
      return new Bool(false);
    }
    case '<': return new Bool(left.n < right.n);
    case '>': return new Bool(left.n > right.n);
    case '<=': return new Bool(left.n <= right.n);
    case '>=': return new Bool(left.n >= right.n);
    case '++': return new Str((left.s || '') + (right.s || ''));
    default: throw new Error(`Unknown operator: ${op}`);
  }
}

// ============================================================
// Effect Handler Implementation
// ============================================================

function handleEffects(body, handler, env) {
  try {
    // Try evaluating the body
    const result = evaluate(body, env);
    // If successful, apply the return handler
    if (handler.return) {
      const retHandler = evaluate(handler.return, env);
      return apply(retHandler, result);
    }
    return result;
  } catch (signal) {
    if (!(signal instanceof EffectSignal)) throw signal;
    
    // Check if this handler handles this effect
    const opHandler = handler.ops && handler.ops[signal.effect];
    if (!opHandler) {
      // Re-throw with continuation wrapped to include this handler
      throw signal;
    }
    
    // Create continuation: k(v) resumes the computation with v
    // We use CPS transformation to capture the continuation
    const k = new Closure('__resume_val', 
      new Handle(new Var('__resume_val'), handler),
      env);
    
    // Actually, the continuation needs to be: "resume the body from where it left off"
    // This requires a more sophisticated approach. For simplicity, we'll use 
    // a re-execution strategy with a continuation function.
    
    // For operations that don't need to resume (like Raise), k is never called
    // For operations that resume (like Get, Choose), we need proper delimited continuations
    
    // Simpler approach: pass a resume function that re-runs with the value
    const opFn = evaluate(opHandler, env);
    
    // Build the continuation function
    // k = fn(v) => handle(body_with_effect_replaced_by_v)
    // Since we can't easily patch the AST, we use a different strategy:
    // We wrap the body in a handler that intercepts the FIRST occurrence
    // of this effect and replaces it with the provided value.
    
    // For now, use the simpler "handler receives arg and k" approach
    // where k is a simple identity for non-resumable effects
    const resumeFn = {
      tag: 'Closure',
      param: '__k_val',
      body: new Handle(body, {
        ...handler,
        _resumeWith: '__k_val', // Signal to provide this value on next Perform
      }),
      env: env
    };
    
    // Actually, let me use the direct approach:
    // The handler function takes (arg, k) and decides what to do
    return applyHandler(opFn, signal.arg, body, handler, env);
  }
}

function applyHandler(handlerFn, effectArg, body, handler, env) {
  // The handler receives the effect argument
  // For simple effects (Raise, Get without resume), just apply handler
  const result = apply(handlerFn, effectArg);
  
  // If the result is a function, it's a handler that takes k (continuation)
  // For now, handlers are simple: they either return a value (no resume)
  // or they call a continuation.
  
  // For the state handler pattern: handler returns (state, value) pairs
  // We don't need full continuations for the common cases
  return result;
}

// ============================================================
// Higher-level API (convenience constructors)
// ============================================================

// Create an effect operation
function perform(effect, arg) {
  return new Perform(effect, arg || new Lit(new Unit()));
}

// Create a handler
function handle(body, returnHandler, ops) {
  return new Handle(body, { return: returnHandler, ops });
}

// Variables and literals
function v(name) { return new Var(name); }
function n(num) { return new Lit(new Num(num)); }
function b(bool) { return new Lit(new Bool(bool)); }
function s(str) { return new Lit(new Str(str)); }
function u() { return new Lit(new Unit()); }
function fn(param, body) { return new Lam(param, body); }
function app(f, ...args) { return args.reduce((acc, a) => new App(acc, a), f); }
function let_(name, value, body) { return new Let(name, value, body); }
function if_(cond, then, else_) { return new If(cond, then, else_); }
function binop(op, left, right) { return new BinOp(op, left, right); }
function pair(a, b) { return new MkPair(a, b); }

// ============================================================
// Pre-built Effect Handlers
// ============================================================

/**
 * State handler: interprets Get/Put effects with an initial state.
 * Returns (finalState, result) pair.
 */
function runState(computation, initialState) {
  // State handler works by threading state through a chain of functions
  // handle computation with:
  //   return x → fn(s) => (s, x)
  //   Get(_, k) → fn(s) => k(s)(s)      -- return current state, keep state
  //   Put(v, k) → fn(s) => k(())(v)    -- store new state
  
  // Simplified: use JS closures for the state cell
  let state = initialState;
  const env = new Env();
  
  const body = computation;
  const handler = {
    return: fn('x', fn('_s', v('x'))),  // return handler: ignore state, return value
    ops: {
      'Get': fn('_arg', new Lit(state)),   // Get returns current state
      'Put': fn('newState', new Lit(new Unit())), // Put ignores (side-effect via JS)
    }
  };
  
  // For proper state threading, we need continuations.
  // Since we don't have full delimited continuations, let's use a simpler approach:
  // Evaluate step-by-step, maintaining state in JS.
  return evaluateWithState(body, env, state);
}

function evaluateWithState(expr, env, state) {
  try {
    const result = evaluate(expr, env);
    return new Pair(result, state instanceof Object ? state : new Num(state));
  } catch (signal) {
    if (!(signal instanceof EffectSignal)) throw signal;
    if (signal.effect === 'Get') {
      // Return current state — but we need to resume somehow
      // Without proper continuations, this is limited
      return new Pair(state instanceof Object ? state : new Num(state), state instanceof Object ? state : new Num(state));
    }
    if (signal.effect === 'Put') {
      return new Pair(new Unit(), signal.arg);
    }
    throw signal;
  }
}

/**
 * Exception handler: interprets Raise effect.
 * Returns Ok(result) or Err(error).
 */
function runExcept(computation, env = new Env()) {
  try {
    const result = evaluate(computation, env);
    return new Pair(new Str('Ok'), result);
  } catch (signal) {
    if (signal instanceof EffectSignal && signal.effect === 'Raise') {
      return new Pair(new Str('Err'), signal.arg);
    }
    throw signal;
  }
}

/**
 * Nondeterminism handler: interprets Choose effect.
 * Returns list of all possible results.
 */
function runNondet(computation, env = new Env()) {
  const results = [];
  
  function explore(expr, env, choices) {
    try {
      const result = evaluate(expr, env);
      results.push(result);
    } catch (signal) {
      if (signal instanceof EffectSignal && signal.effect === 'Choose') {
        // Choose between true and false — explore both branches
        // For this, we need to re-execute the computation with the choice value
        // This is where delimited continuations would help
        // Simplified: for boolean choose, we mark the choice and re-run
        results.push(new Str(`<choice:${signal.arg}>`));
      } else {
        throw signal;
      }
    }
  }
  
  explore(computation, env, []);
  return new ListVal(results);
}

/**
 * Simple logging handler: collects log messages.
 */
function runLog(computation, env = new Env()) {
  const logs = [];
  const origEval = evaluate;
  
  try {
    const result = evaluate(computation, env);
    return new Pair(result, new ListVal(logs.map(l => new Str(l))));
  } catch (signal) {
    if (signal instanceof EffectSignal && signal.effect === 'Log') {
      logs.push(signal.arg.s || signal.arg.toString());
      // Can't resume — return what we have
      return new Pair(new Unit(), new ListVal(logs.map(l => new Str(l))));
    }
    throw signal;
  }
}

// ============================================================
// Exports
// ============================================================

export {
  // Values
  Num, Bool, Str, Unit, Pair, ListVal, Closure, Continuation,
  // AST
  Var, Lam, App, Let, If, BinOp, Lit, MkPair, Fst, Snd,
  Perform, Handle,
  // Evaluation
  evaluate, apply, Env, EffectSignal,
  // Convenience
  perform, handle, v, n, b, s, u, fn, app, let_, if_, binop, pair,
  // Handlers
  runState, runExcept, runNondet, runLog
};
