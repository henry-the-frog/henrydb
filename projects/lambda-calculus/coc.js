/**
 * Calculus of Constructions (CoC)
 * 
 * The apex of the lambda cube. Extends System F with dependent types.
 * 
 * Features:
 * - Pi types Π(x:A).B — subsumes both → and ∀
 * - Two sorts: Type (★) and Kind (□)
 * - Dependent function types (return type depends on argument value)
 * - Type-level computation
 * - Bidirectional type checking (check/infer)
 * 
 * Typing rules:
 *   (★ : □)                              — Type is a Kind
 *   Γ ⊢ A : s₁   Γ, x:A ⊢ B : s₂       — Pi formation (s₁,s₂ ∈ {★,□})
 *   ──────────────────────────────         
 *   Γ ⊢ Π(x:A).B : s₂                   
 *   
 *   Γ, x:A ⊢ t : B                       — Lambda introduction
 *   ────────────────────
 *   Γ ⊢ λ(x:A).t : Π(x:A).B
 *   
 *   Γ ⊢ f : Π(x:A).B   Γ ⊢ a : A        — Application elimination
 *   ───────────────────────────────
 *   Γ ⊢ f a : B[x := a]
 * 
 * Strongly normalizing: all well-typed terms terminate.
 * Consistent as a logic (via Curry-Howard).
 */

// ============================================================
// Terms
// ============================================================

// Sorts
class Star {
  constructor() { this.tag = 'Star'; }
  toString() { return '★'; }
  equals(other) { return other instanceof Star; }
}

class Box {
  constructor() { this.tag = 'Box'; }
  toString() { return '□'; }
  equals(other) { return other instanceof Box; }
}

// Variable
class Var {
  constructor(name) { this.tag = 'Var'; this.name = name; }
  toString() { return this.name; }
  equals(other) { return other instanceof Var && other.name === this.name; }
}

// Pi type: Π(x:A).B — when x not free in B, equivalent to A → B
class Pi {
  constructor(param, paramType, body) {
    this.tag = 'Pi';
    this.param = param;       // string
    this.paramType = paramType; // Term
    this.body = body;           // Term
  }
  toString() {
    if (!freeIn(this.param, this.body)) {
      // Non-dependent: show as A → B
      const a = this.paramType instanceof Pi || this.paramType instanceof Lam
        ? `(${this.paramType})` : `${this.paramType}`;
      return `${a} → ${this.body}`;
    }
    return `Π(${this.param}:${this.paramType}).${this.body}`;
  }
  equals(other) {
    if (!(other instanceof Pi)) return false;
    if (!this.paramType.equals(other.paramType)) return false;
    // Alpha equivalence
    if (this.param === other.param) return this.body.equals(other.body);
    const fresh = freshName(this.param);
    return subst(this.body, this.param, new Var(fresh)).equals(
      subst(other.body, other.param, new Var(fresh)));
  }
}

// Lambda: λ(x:A).t
class Lam {
  constructor(param, paramType, body) {
    this.tag = 'Lam';
    this.param = param;
    this.paramType = paramType;
    this.body = body;
  }
  toString() { return `λ(${this.param}:${this.paramType}).${this.body}`; }
  equals(other) {
    if (!(other instanceof Lam)) return false;
    if (!this.paramType.equals(other.paramType)) return false;
    if (this.param === other.param) return this.body.equals(other.body);
    const fresh = freshName(this.param);
    return subst(this.body, this.param, new Var(fresh)).equals(
      subst(other.body, other.param, new Var(fresh)));
  }
}

// Application: f a
class App {
  constructor(fn, arg) {
    this.tag = 'App';
    this.fn = fn;
    this.arg = arg;
  }
  toString() {
    const f = this.fn instanceof Lam || this.fn instanceof Pi ? `(${this.fn})` : `${this.fn}`;
    const a = this.arg instanceof App || this.arg instanceof Lam || this.arg instanceof Pi
      ? `(${this.arg})` : `${this.arg}`;
    return `${f} ${a}`;
  }
  equals(other) {
    return other instanceof App && this.fn.equals(other.fn) && this.arg.equals(other.arg);
  }
}

// Nat type and constructors (built-in for examples)
class Nat {
  constructor() { this.tag = 'Nat'; }
  toString() { return 'ℕ'; }
  equals(other) { return other instanceof Nat; }
}

class Zero {
  constructor() { this.tag = 'Zero'; }
  toString() { return '0'; }
  equals(other) { return other instanceof Zero; }
}

class Succ {
  constructor(n) { this.tag = 'Succ'; this.n = n; }
  toString() {
    // Pretty-print small numbers
    let count = 0, current = this;
    while (current instanceof Succ) { count++; current = current.n; }
    if (current instanceof Zero) return `${count}`;
    return `S(${this.n})`;
  }
  equals(other) { return other instanceof Succ && this.n.equals(other.n); }
}

// Nat eliminator: natElim(P, z, s, n)
// P : ℕ → ★, z : P 0, s : Π(k:ℕ).P k → P (S k), n : ℕ
class NatElim {
  constructor(motive, zero, succ, scrutinee) {
    this.tag = 'NatElim';
    this.motive = motive;
    this.zero = zero;
    this.succ = succ;
    this.scrutinee = scrutinee;
  }
  toString() { return `natElim(${this.motive}, ${this.zero}, ${this.succ}, ${this.scrutinee})`; }
  equals(other) {
    return other instanceof NatElim &&
      this.motive.equals(other.motive) &&
      this.zero.equals(other.zero) &&
      this.succ.equals(other.succ) &&
      this.scrutinee.equals(other.scrutinee);
  }
}

// ============================================================
// Helper functions
// ============================================================

let nameCounter = 0;
function freshName(base) { return `${base}$${nameCounter++}`; }
function resetNames() { nameCounter = 0; }

function freeVars(term) {
  const vars = new Set();
  function go(t) {
    if (t instanceof Var) vars.add(t.name);
    else if (t instanceof Pi) { go(t.paramType); go(t.body); vars.delete(t.param); }
    else if (t instanceof Lam) { go(t.paramType); go(t.body); vars.delete(t.param); }
    else if (t instanceof App) { go(t.fn); go(t.arg); }
    else if (t instanceof Succ) go(t.n);
    else if (t instanceof NatElim) { go(t.motive); go(t.zero); go(t.succ); go(t.scrutinee); }
  }
  go(term);
  return vars;
}

function freeIn(name, term) { return freeVars(term).has(name); }

function subst(term, name, replacement) {
  if (term instanceof Star || term instanceof Box) return term;
  if (term instanceof Nat || term instanceof Zero) return term;
  if (term instanceof Var) return term.name === name ? replacement : term;
  if (term instanceof Succ) return new Succ(subst(term.n, name, replacement));
  if (term instanceof App) return new App(subst(term.fn, name, replacement), subst(term.arg, name, replacement));
  if (term instanceof NatElim) {
    return new NatElim(
      subst(term.motive, name, replacement),
      subst(term.zero, name, replacement),
      subst(term.succ, name, replacement),
      subst(term.scrutinee, name, replacement)
    );
  }
  if (term instanceof Pi || term instanceof Lam) {
    const paramType = subst(term.paramType, name, replacement);
    if (term.param === name) {
      // Shadowed — don't substitute in body
      return term instanceof Pi 
        ? new Pi(term.param, paramType, term.body)
        : new Lam(term.param, paramType, term.body);
    }
    // Avoid capture
    const fv = freeVars(replacement);
    let param = term.param, body = term.body;
    if (fv.has(param)) {
      const fresh = freshName(param);
      body = subst(body, param, new Var(fresh));
      param = fresh;
    }
    body = subst(body, name, replacement);
    return term instanceof Pi ? new Pi(param, paramType, body) : new Lam(param, paramType, body);
  }
  return term;
}

// ============================================================
// Normalization (beta reduction to normal form)
// ============================================================

function normalize(term) {
  if (term instanceof Star || term instanceof Box) return term;
  if (term instanceof Nat || term instanceof Zero) return term;
  if (term instanceof Var) return term;
  if (term instanceof Succ) return new Succ(normalize(term.n));
  
  if (term instanceof Lam) {
    return new Lam(term.param, normalize(term.paramType), normalize(term.body));
  }
  
  if (term instanceof Pi) {
    return new Pi(term.param, normalize(term.paramType), normalize(term.body));
  }
  
  if (term instanceof App) {
    const fn = normalize(term.fn);
    const arg = normalize(term.arg);
    if (fn instanceof Lam) {
      // Beta reduction
      return normalize(subst(fn.body, fn.param, arg));
    }
    return new App(fn, arg);
  }
  
  if (term instanceof NatElim) {
    const motive = normalize(term.motive);
    const zero = normalize(term.zero);
    const succ = normalize(term.succ);
    const n = normalize(term.scrutinee);
    
    if (n instanceof Zero) return zero;
    if (n instanceof Succ) {
      // natElim(P, z, s, S k) → s k (natElim(P, z, s, k))
      const rec = normalize(new NatElim(motive, zero, succ, n.n));
      return normalize(new App(new App(succ, n.n), rec));
    }
    return new NatElim(motive, zero, succ, n);
  }
  
  return term;
}

// Beta-equivalence: normalize both sides and compare
function betaEq(a, b) {
  return normalize(a).equals(normalize(b));
}

// ============================================================
// Type Checking (Bidirectional)
// ============================================================

class Context {
  constructor(bindings = []) { this.bindings = bindings; }
  
  extend(name, type) {
    return new Context([...this.bindings, { name, type }]);
  }
  
  lookup(name) {
    for (let i = this.bindings.length - 1; i >= 0; i--) {
      if (this.bindings[i].name === name) return this.bindings[i].type;
    }
    return null;
  }
}

class TypeError extends Error {
  constructor(msg) { super(msg); this.name = 'TypeError'; }
}

// Infer the type of a term
function infer(ctx, term) {
  // (★ : □)
  if (term instanceof Star) return new Box();
  
  // Nat : ★
  if (term instanceof Nat) return new Star();
  if (term instanceof Zero) return new Nat();
  if (term instanceof Succ) {
    const nType = infer(ctx, term.n);
    if (!betaEq(nType, new Nat())) {
      throw new TypeError(`Succ argument must be ℕ, got ${nType}`);
    }
    return new Nat();
  }
  
  // Variable: look up in context
  if (term instanceof Var) {
    const type = ctx.lookup(term.name);
    if (!type) throw new TypeError(`Unbound variable: ${term.name}`);
    return type;
  }
  
  // Pi type formation
  if (term instanceof Pi) {
    const aSort = infer(ctx, term.paramType);
    const normASort = normalize(aSort);
    if (!(normASort instanceof Star) && !(normASort instanceof Box)) {
      throw new TypeError(`Pi domain type must be a sort, got ${normASort}`);
    }
    const extCtx = ctx.extend(term.param, term.paramType);
    const bSort = infer(extCtx, term.body);
    const normBSort = normalize(bSort);
    if (!(normBSort instanceof Star) && !(normBSort instanceof Box)) {
      throw new TypeError(`Pi codomain type must be a sort, got ${normBSort}`);
    }
    return normBSort;
  }
  
  // Lambda: infer Pi type
  if (term instanceof Lam) {
    // Check that param type is valid
    const aSort = infer(ctx, term.paramType);
    const normASort = normalize(aSort);
    if (!(normASort instanceof Star) && !(normASort instanceof Box)) {
      throw new TypeError(`Lambda parameter type must be a sort, got ${normASort}`);
    }
    const extCtx = ctx.extend(term.param, term.paramType);
    const bodyType = infer(extCtx, term.body);
    return new Pi(term.param, term.paramType, bodyType);
  }
  
  // Application
  if (term instanceof App) {
    const fnType = normalize(infer(ctx, term.fn));
    if (!(fnType instanceof Pi)) {
      throw new TypeError(`Application requires Pi type, got ${fnType}`);
    }
    check(ctx, term.arg, fnType.paramType);
    // Return type with argument substituted in
    return normalize(subst(fnType.body, fnType.param, term.arg));
  }
  
  // NatElim
  if (term instanceof NatElim) {
    // motive : ℕ → ★
    const motiveType = infer(ctx, term.motive);
    const expectedMotiveType = new Pi('_n', new Nat(), new Star());
    if (!betaEq(motiveType, expectedMotiveType)) {
      throw new TypeError(`natElim motive must be ℕ → ★, got ${motiveType}`);
    }
    
    // zero case : P 0
    const zeroType = normalize(new App(term.motive, new Zero()));
    check(ctx, term.zero, zeroType);
    
    // succ case : Π(k:ℕ). P k → P (S k)
    const k = freshName('k');
    const succType = new Pi(k, new Nat(),
      new Pi('_ih', new App(term.motive, new Var(k)),
        new App(term.motive, new Succ(new Var(k)))));
    check(ctx, term.succ, succType);
    
    // scrutinee : ℕ
    check(ctx, term.scrutinee, new Nat());
    
    // Result: P n
    return normalize(new App(term.motive, term.scrutinee));
  }
  
  throw new TypeError(`Cannot infer type of: ${term}`);
}

// Check a term against an expected type
function check(ctx, term, expectedType) {
  const inferredType = infer(ctx, term);
  if (!betaEq(inferredType, expectedType)) {
    throw new TypeError(
      `Type mismatch: expected ${normalize(expectedType)}, got ${normalize(inferredType)}\n` +
      `  in term: ${term}`
    );
  }
}

// ============================================================
// Parser
// ============================================================

function parse(input) {
  const tokens = tokenize(input);
  let pos = 0;
  
  function peek() { return pos < tokens.length ? tokens[pos] : null; }
  function advance() { return tokens[pos++]; }
  function expect(val) {
    const t = advance();
    if (t !== val) throw new Error(`Expected '${val}', got '${t}' at position ${pos}`);
    return t;
  }
  
  function parseExpr() {
    // Check for Pi: Π(x:A).B or (x:A) → B
    if (peek() === 'Π' || peek() === 'Pi') {
      advance();
      expect('(');
      const param = advance();
      expect(':');
      const paramType = parseExpr();
      expect(')');
      expect('.');
      const body = parseExpr();
      return new Pi(param, paramType, body);
    }
    
    // Check for Lambda: λ(x:A).t
    if (peek() === 'λ' || peek() === 'lam') {
      advance();
      expect('(');
      const param = advance();
      expect(':');
      const paramType = parseExpr();
      expect(')');
      expect('.');
      const body = parseExpr();
      return new Lam(param, paramType, body);
    }
    
    // Check for natElim
    if (peek() === 'natElim') {
      advance();
      expect('(');
      const motive = parseExpr();
      expect(',');
      const zero = parseExpr();
      expect(',');
      const succ = parseExpr();
      expect(',');
      const scrutinee = parseExpr();
      expect(')');
      return new NatElim(motive, zero, succ, scrutinee);
    }
    
    return parseArrow();
  }
  
  function parseArrow() {
    let left = parseApp();
    if (peek() === '→' || peek() === '->') {
      advance();
      const right = parseExpr();
      return new Pi('_', left, right);
    }
    return left;
  }
  
  function parseApp() {
    let expr = parseAtom();
    while (peek() && peek() !== ')' && peek() !== ',' && peek() !== '.' && 
           peek() !== ':' && peek() !== '→' && peek() !== '->') {
      const arg = parseAtom();
      expr = new App(expr, arg);
    }
    return expr;
  }
  
  function parseAtom() {
    const t = peek();
    if (t === '(') {
      advance();
      const expr = parseExpr();
      expect(')');
      return expr;
    }
    if (t === '★' || t === 'Type' || t === '*') { advance(); return new Star(); }
    if (t === '□' || t === 'Kind') { advance(); return new Box(); }
    if (t === 'ℕ' || t === 'Nat') { advance(); return new Nat(); }
    if (t === '0' || t === 'zero') { advance(); return new Zero(); }
    if (t === 'S' || t === 'succ') {
      advance();
      const arg = parseAtom();
      return new Succ(arg);
    }
    // Number literal
    if (t && /^\d+$/.test(t)) {
      advance();
      let n = parseInt(t, 10);
      let result = new Zero();
      for (let i = 0; i < n; i++) result = new Succ(result);
      return result;
    }
    if (t && /^[a-zA-Zα-ωΑ-Ω_][a-zA-Zα-ωΑ-Ω_0-9']*$/.test(t)) {
      advance();
      return new Var(t);
    }
    throw new Error(`Unexpected token: '${t}' at position ${pos}`);
  }
  
  const result = parseExpr();
  if (pos < tokens.length) throw new Error(`Unexpected token: ${tokens[pos]}`);
  return result;
}

function tokenize(input) {
  const tokens = [];
  let i = 0;
  while (i < input.length) {
    if (/\s/.test(input[i])) { i++; continue; }
    // Two-char tokens
    if (input.slice(i, i+2) === '->') { tokens.push('->'); i += 2; continue; }
    // Special chars
    if ('().,:/'.includes(input[i]) || input[i] === '★' || input[i] === '□' || 
        input[i] === 'Π' || input[i] === 'λ' || input[i] === '→' || input[i] === 'ℕ') {
      tokens.push(input[i]); i++; continue;
    }
    // Identifiers and numbers
    let j = i;
    while (j < input.length && /[a-zA-Zα-ωΑ-Ω_0-9']/.test(input[j])) j++;
    if (j > i) { tokens.push(input.slice(i, j)); i = j; continue; }
    throw new Error(`Unexpected character: '${input[i]}'`);
  }
  return tokens;
}

// ============================================================
// Convenience constructors
// ============================================================

// Non-dependent function type: A → B
function arrow(a, b) { return new Pi('_', a, b); }

// Church-encoded booleans in CoC
function churchBoolType() { return new Pi('α', new Star(), arrow(new Var('α'), arrow(new Var('α'), new Var('α')))); }
function churchTrue() { return new Lam('α', new Star(), new Lam('x', new Var('α'), new Lam('y', new Var('α'), new Var('x')))); }
function churchFalse() { return new Lam('α', new Star(), new Lam('x', new Var('α'), new Lam('y', new Var('α'), new Var('y')))); }

// Identity function: Λ(A:★).λ(x:A).x
function identity() { return new Lam('A', new Star(), new Lam('x', new Var('A'), new Var('x'))); }

// ============================================================
// Exports
// ============================================================

export {
  Star, Box, Var, Pi, Lam, App, Nat, Zero, Succ, NatElim,
  Context, TypeError,
  infer, check, normalize, betaEq, subst, freeVars, freeIn,
  parse, tokenize, freshName, resetNames, arrow,
  churchBoolType, churchTrue, churchFalse, identity
};
