/**
 * Lambda Calculus Interpreter
 * 
 * Pure untyped lambda calculus with:
 * - Parser (λx.body and \x.body syntax)
 * - Multiple reduction strategies (normal-order, applicative-order, call-by-name, call-by-value)
 * - De Bruijn index representation
 * - Church encodings (numerals, booleans, pairs, lists)
 * - Y combinator and recursion
 * - Alpha-equivalence checking
 * - Step-by-step reduction tracing
 */

// ============================================================
// AST Nodes
// ============================================================

class Var {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
  equals(other) { return other instanceof Var && other.name === this.name; }
  clone() { return new Var(this.name); }
}

class Abs {
  constructor(param, body) { this.param = param; this.body = body; }
  toString() { return `(λ${this.param}.${this.body})`; }
  equals(other) { return other instanceof Abs && other.param === this.param && this.body.equals(other.body); }
  clone() { return new Abs(this.param, this.body.clone()); }
}

class App {
  constructor(func, arg) { this.func = func; this.arg = arg; }
  toString() { return `(${this.func} ${this.arg})`; }
  equals(other) { return other instanceof App && this.func.equals(other.func) && this.arg.equals(other.arg); }
  clone() { return new App(this.func.clone(), this.arg.clone()); }
}

// ============================================================
// De Bruijn Index Representation
// ============================================================

class DeBruijnVar {
  constructor(index) { this.index = index; }
  toString() { return String(this.index); }
  equals(other) { return other instanceof DeBruijnVar && other.index === this.index; }
}

class DeBruijnAbs {
  constructor(body) { this.body = body; }
  toString() { return `(λ.${this.body})`; }
  equals(other) { return other instanceof DeBruijnAbs && this.body.equals(other.body); }
}

class DeBruijnApp {
  constructor(func, arg) { this.func = func; this.arg = arg; }
  toString() { return `(${this.func} ${this.arg})`; }
  equals(other) { return other instanceof DeBruijnApp && this.func.equals(other.func) && this.arg.equals(other.arg); }
}

// ============================================================
// Tokenizer
// ============================================================

function tokenize(input) {
  const tokens = [];
  let i = 0;
  while (i < input.length) {
    const ch = input[i];
    if (/\s/.test(ch)) { i++; continue; }
    if (ch === '(' || ch === ')' || ch === '.' || ch === 'λ' || ch === '\\') {
      tokens.push(ch === '\\' ? 'λ' : ch);
      i++;
      continue;
    }
    // Identifier: letters, digits, underscore, primes
    if (/[a-zA-Z_0-9']/.test(ch)) {
      let id = '';
      while (i < input.length && /[a-zA-Z_0-9']/.test(input[i])) {
        id += input[i++];
      }
      tokens.push(id);
      continue;
    }
    throw new Error(`Unexpected character: '${ch}' at position ${i}`);
  }
  return tokens;
}

// ============================================================
// Parser
// ============================================================

function parse(input) {
  const tokens = tokenize(input);
  let pos = 0;

  function peek() { return tokens[pos]; }
  function advance() { return tokens[pos++]; }
  function expect(tok) {
    if (peek() !== tok) throw new Error(`Expected '${tok}', got '${peek()}' at position ${pos}`);
    return advance();
  }

  // expr = atom+  (left-associative application)
  function parseExpr() {
    let node = parseAtom();
    while (pos < tokens.length && peek() !== ')') {
      node = new App(node, parseAtom());
    }
    return node;
  }

  // atom = '(' expr ')' | 'λ' params '.' expr | variable
  function parseAtom() {
    const tok = peek();
    if (tok === '(') {
      advance(); // '('
      const expr = parseExpr();
      expect(')');
      return expr;
    }
    if (tok === 'λ') {
      advance(); // 'λ'
      // Multi-param: λx y z. body  →  λx.(λy.(λz.body))
      const params = [];
      while (peek() !== '.') {
        params.push(advance());
      }
      expect('.'); // '.'
      const body = parseExpr();
      // Desugar multi-param
      let result = body;
      for (let i = params.length - 1; i >= 0; i--) {
        result = new Abs(params[i], result);
      }
      return result;
    }
    // Variable
    if (tok && tok !== ')' && tok !== '.') {
      advance();
      return new Var(tok);
    }
    throw new Error(`Unexpected token: '${tok}' at position ${pos}`);
  }

  const result = parseExpr();
  if (pos < tokens.length) {
    throw new Error(`Unexpected token '${tokens[pos]}' at position ${pos}`);
  }
  return result;
}

// ============================================================
// Free Variables
// ============================================================

function freeVars(expr) {
  if (expr instanceof Var) return new Set([expr.name]);
  if (expr instanceof Abs) {
    const fv = freeVars(expr.body);
    fv.delete(expr.param);
    return fv;
  }
  if (expr instanceof App) {
    const fv1 = freeVars(expr.func);
    const fv2 = freeVars(expr.arg);
    for (const v of fv2) fv1.add(v);
    return fv1;
  }
  return new Set();
}

// ============================================================
// Alpha-Equivalence (using de Bruijn indices)
// ============================================================

function alphaEquivalent(a, b) {
  return toDeBruijn(a).equals(toDeBruijn(b));
}

// ============================================================
// Substitution (capture-avoiding)
// ============================================================

let freshCounter = 0;
function freshVar(base) {
  return `${base}'${freshCounter++}`;
}

function resetFreshCounter() { freshCounter = 0; }

function substitute(expr, varName, replacement) {
  if (expr instanceof Var) {
    return expr.name === varName ? replacement.clone() : expr;
  }
  if (expr instanceof App) {
    return new App(
      substitute(expr.func, varName, replacement),
      substitute(expr.arg, varName, replacement)
    );
  }
  if (expr instanceof Abs) {
    // If the param shadows the variable, no substitution needed
    if (expr.param === varName) return expr;
    // Check for capture: if the param appears free in the replacement
    const fv = freeVars(replacement);
    if (fv.has(expr.param)) {
      // Alpha-rename to avoid capture
      const newParam = freshVar(expr.param);
      const renamedBody = substitute(expr.body, expr.param, new Var(newParam));
      return new Abs(newParam, substitute(renamedBody, varName, replacement));
    }
    return new Abs(expr.param, substitute(expr.body, varName, replacement));
  }
  return expr;
}

// ============================================================
// Reduction Strategies
// ============================================================

// Beta-reduce a single redex (App(Abs(...), arg))
function betaReduce(abs, arg) {
  return substitute(abs.body, abs.param, arg);
}

// Normal-order reduction: leftmost-outermost redex first
// Returns null if no reduction possible (normal form)
function normalOrderStep(expr) {
  if (expr instanceof App) {
    // If func is abstraction, this is a redex — reduce it
    if (expr.func instanceof Abs) {
      return betaReduce(expr.func, expr.arg);
    }
    // Try reducing func first (leftmost-outermost)
    const reducedFunc = normalOrderStep(expr.func);
    if (reducedFunc !== null) {
      return new App(reducedFunc, expr.arg);
    }
    // Then try reducing arg
    const reducedArg = normalOrderStep(expr.arg);
    if (reducedArg !== null) {
      return new App(expr.func, reducedArg);
    }
    return null;
  }
  if (expr instanceof Abs) {
    // Reduce under lambda (full normal form)
    const reducedBody = normalOrderStep(expr.body);
    if (reducedBody !== null) {
      return new Abs(expr.param, reducedBody);
    }
    return null;
  }
  // Var — no reduction
  return null;
}

// Applicative-order reduction: leftmost-innermost redex first
function applicativeOrderStep(expr) {
  if (expr instanceof App) {
    // Reduce arg first (innermost)
    const reducedArg = applicativeOrderStep(expr.arg);
    if (reducedArg !== null) {
      return new App(expr.func, reducedArg);
    }
    // Then reduce func
    const reducedFunc = applicativeOrderStep(expr.func);
    if (reducedFunc !== null) {
      return new App(reducedFunc, expr.arg);
    }
    // If both are in normal form and func is lambda, reduce
    if (expr.func instanceof Abs) {
      return betaReduce(expr.func, expr.arg);
    }
    return null;
  }
  if (expr instanceof Abs) {
    const reducedBody = applicativeOrderStep(expr.body);
    if (reducedBody !== null) {
      return new Abs(expr.param, reducedBody);
    }
    return null;
  }
  return null;
}

// Call-by-value: only reduce when argument is a value (variable or abstraction)
function callByValueStep(expr) {
  if (expr instanceof App) {
    // Reduce func first
    const reducedFunc = callByValueStep(expr.func);
    if (reducedFunc !== null) {
      return new App(reducedFunc, expr.arg);
    }
    // Then reduce arg
    const reducedArg = callByValueStep(expr.arg);
    if (reducedArg !== null) {
      return new App(expr.func, reducedArg);
    }
    // Beta-reduce only if arg is a value
    if (expr.func instanceof Abs && isValue(expr.arg)) {
      return betaReduce(expr.func, expr.arg);
    }
    return null;
  }
  // Don't reduce under lambda in CBV
  return null;
}

function isValue(expr) {
  return expr instanceof Var || expr instanceof Abs;
}

// Call-by-name: don't evaluate the argument before substitution
function callByNameStep(expr) {
  if (expr instanceof App) {
    if (expr.func instanceof Abs) {
      return betaReduce(expr.func, expr.arg);
    }
    const reducedFunc = callByNameStep(expr.func);
    if (reducedFunc !== null) {
      return new App(reducedFunc, expr.arg);
    }
    return null;
  }
  // Don't reduce under lambda or in arguments
  return null;
}

// ============================================================
// Multi-step Reduction
// ============================================================

function reduce(expr, strategy = 'normal', maxSteps = 1000) {
  const stepFn = {
    'normal': normalOrderStep,
    'applicative': applicativeOrderStep,
    'cbv': callByValueStep,
    'cbn': callByNameStep,
  }[strategy];

  if (!stepFn) throw new Error(`Unknown strategy: ${strategy}`);

  let current = expr;
  let steps = 0;
  const trace = [current.toString()];

  while (steps < maxSteps) {
    const next = stepFn(current);
    if (next === null) break; // Normal form reached
    current = next;
    steps++;
    trace.push(current.toString());
  }

  return { result: current, steps, trace, normalForm: steps < maxSteps };
}

// ============================================================
// De Bruijn Index Conversion
// ============================================================

function toDeBruijn(expr, env = []) {
  if (expr instanceof Var) {
    const idx = env.indexOf(expr.name);
    if (idx === -1) {
      // Free variable — use a large index (depth + name hash)
      return new DeBruijnVar(env.length + expr.name.charCodeAt(0));
    }
    return new DeBruijnVar(idx);
  }
  if (expr instanceof Abs) {
    return new DeBruijnAbs(toDeBruijn(expr.body, [expr.param, ...env]));
  }
  if (expr instanceof App) {
    return new DeBruijnApp(toDeBruijn(expr.func, env), toDeBruijn(expr.arg, env));
  }
}

function fromDeBruijn(expr, names = []) {
  if (expr instanceof DeBruijnVar) {
    if (expr.index < names.length) return new Var(names[expr.index]);
    return new Var(`free_${expr.index}`);
  }
  if (expr instanceof DeBruijnAbs) {
    const name = nextName(names);
    return new Abs(name, fromDeBruijn(expr.body, [name, ...names]));
  }
  if (expr instanceof DeBruijnApp) {
    return new App(fromDeBruijn(expr.func, names), fromDeBruijn(expr.arg, names));
  }
}

const namePool = 'xyzwvutsrqponmlkjihgfedcba';
function nextName(used) {
  for (const ch of namePool) {
    if (!used.includes(ch)) return ch;
  }
  return `v${used.length}`;
}

// ============================================================
// Church Encodings
// ============================================================

const church = {
  // Booleans
  true: parse('λt f.t'),
  false: parse('λt f.f'),
  and: parse('λp q.p q p'),
  or: parse('λp q.p p q'),
  not: parse('λp.p (λt f.f) (λt f.t)'),
  ifthenelse: parse('λp a b.p a b'),

  // Numbers
  zero: parse('λf x.x'),
  one: parse('λf x.f x'),
  two: parse('λf x.f (f x)'),
  three: parse('λf x.f (f (f x))'),
  succ: parse('λn f x.f (n f x)'),
  plus: parse('λm n f x.m f (n f x)'),
  mult: parse('λm n f.m (n f)'),
  exp: parse('λm n.n m'),
  pred: parse('λn f x.n (λg h.h (g f)) (λu.x) (λu.u)'),
  sub: parse('λm n.n (λn f x.n (λg h.h (g f)) (λu.x) (λu.u)) m'),
  isZero: parse('λn.n (λx.λt f.f) (λt f.t)'),

  // Pairs
  pair: parse('λa b f.f a b'),
  fst: parse('λp.p (λa b.a)'),
  snd: parse('λp.p (λa b.b)'),

  // Lists (Scott encoding)
  nil: parse('λc n.n'),
  cons: parse('λh t c n.c h (t c n)'),
  head: parse('λl.l (λh t.h) (λx.x)'),
  tail: parse('λl c n.l (λh t g.g h (t c n)) (λt.t (λh t.t)) (λh t.t)'),
  isNil: parse('λl.l (λh t.λt f.f) (λt f.t)'),
  length: parse('λl.l (λh t.λf x.f (t f x)) (λf x.x)'),

  // Y combinator (fixed-point)
  Y: parse('λf.(λx.f (x x)) (λx.f (x x))'),
  // Z combinator (strict/CBV fixed-point)
  Z: parse('λf.(λx.f (λv.x x v)) (λx.f (λv.x x v))'),

  // Omega (non-terminating)
  omega: parse('(λx.x x) (λx.x x)'),
};

// Helper: build a Church numeral for any non-negative integer
function churchNumeral(n) {
  if (n === 0) return parse('λf x.x');
  // λf x. f (f (... (f x)...))
  let body = new Var('x');
  for (let i = 0; i < n; i++) {
    body = new App(new Var('f'), body);
  }
  return new Abs('f', new Abs('x', body));
}

// Helper: decode a Church numeral back to an integer
function unchurch(expr) {
  // Apply the numeral to (x => x + 1) and 0
  // We do this by reducing: expr (λx.SUCC x) ZERO
  // But simpler: count the nested f applications
  const reduced = reduce(expr, 'normal', 2000).result;
  // Should be λf.λx. f (f (... x))
  if (!(reduced instanceof Abs)) return null;
  if (!(reduced.body instanceof Abs)) return null;
  let count = 0;
  let current = reduced.body.body;
  const f = reduced.param;
  const x = reduced.body.param;
  while (current instanceof App && current.func instanceof Var && current.func.name === f) {
    count++;
    current = current.arg;
  }
  if (current instanceof Var && current.name === x) return count;
  return null;
}

// Helper: decode a Church boolean
function unchurchBool(expr) {
  const reduced = reduce(expr, 'normal', 1000).result;
  // TRUE = λt f.t, FALSE = λt f.f
  if (reduced instanceof Abs && reduced.body instanceof Abs) {
    if (reduced.body.body instanceof Var) {
      if (reduced.body.body.name === reduced.param) return true;
      if (reduced.body.body.name === reduced.body.param) return false;
    }
  }
  return null;
}

// ============================================================
// Pretty Printing
// ============================================================

function prettyPrint(expr, minimal = false) {
  if (expr instanceof Var) return expr.name;
  if (expr instanceof Abs) {
    if (minimal) {
      // Collect consecutive lambdas
      let params = [expr.param];
      let body = expr.body;
      while (body instanceof Abs) {
        params.push(body.param);
        body = body.body;
      }
      return `λ${params.join(' ')}.${prettyPrint(body, true)}`;
    }
    return `(λ${expr.param}.${prettyPrint(expr.body, minimal)})`;
  }
  if (expr instanceof App) {
    const funcStr = expr.func instanceof Abs
      ? `(${prettyPrint(expr.func, minimal)})`
      : prettyPrint(expr.func, minimal);
    const argStr = expr.arg instanceof App
      ? `(${prettyPrint(expr.arg, minimal)})`
      : prettyPrint(expr.arg, minimal);
    return `${funcStr} ${argStr}`;
  }
}

// ============================================================
// Exports
// ============================================================

export {
  // AST
  Var, Abs, App,
  // De Bruijn
  DeBruijnVar, DeBruijnAbs, DeBruijnApp,
  toDeBruijn, fromDeBruijn,
  // Parser
  parse, tokenize,
  // Operations
  freeVars, substitute, alphaEquivalent,
  betaReduce, reduce,
  normalOrderStep, applicativeOrderStep, callByValueStep, callByNameStep,
  // Church encodings
  church, churchNumeral, unchurch, unchurchBool,
  // Utils
  prettyPrint, resetFreshCounter, isValue,
};
