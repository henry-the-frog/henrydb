// type-inference/types.js — Hindley-Milner type inference
// Algorithm W with Robinson unification
// ─── Types ───
// TVar(name)           — type variable: α, β, γ
// TCon(name)           — type constant: Int, Bool, String
// TFun(param, result)  — function type: τ₁ → τ₂
// TList(elem)          — list type: [τ]
// TPair(fst, snd)      — pair type: (τ₁, τ₂)

class TVar {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
}

class TCon {
  constructor(name) { this.name = name; }
  toString() { return this.name; }
}

class TFun {
  constructor(param, result) { this.param = param; this.result = result; }
  toString() {
    const p = this.param instanceof TFun ? `(${this.param})` : `${this.param}`;
    return `${p} -> ${this.result}`;
  }
}

class TList {
  constructor(elem) { this.elem = elem; }
  toString() { return `[${this.elem}]`; }
}

class TPair {
  constructor(fst, snd) { this.fst = fst; this.snd = snd; }
  toString() { return `(${this.fst}, ${this.snd})`; }
}

// Common type constants
const tInt = new TCon('Int');
const tBool = new TCon('Bool');
const tString = new TCon('String');
const tUnit = new TCon('Unit');

// ─── Type Scheme ───
// ∀ α₁ α₂ ... αₙ. τ
// Quantified type variables in a polymorphic type
class Scheme {
  constructor(vars, type) { this.vars = vars; this.type = type; }
  toString() {
    if (this.vars.length === 0) return `${this.type}`;
    return `∀${this.vars.join(' ')}. ${this.type}`;
  }
}

// ─── Substitution ───
// Maps type variable names to types
class Subst {
  constructor(map = new Map()) { this.map = map; }

  static empty() { return new Subst(); }
  static single(name, type) { return new Subst(new Map([[name, type]])); }

  lookup(name) { return this.map.get(name); }

  // Apply substitution to a type
  apply(type) {
    if (type instanceof TVar) {
      const t = this.map.get(type.name);
      if (!t) return type;
      // Guard against self-referencing substitution (a → TVar(a))
      if (t instanceof TVar && t.name === type.name) return type;
      return this.apply(t);
    }
    if (type instanceof TCon) return type;
    if (type instanceof TFun) return new TFun(this.apply(type.param), this.apply(type.result));
    if (type instanceof TList) return new TList(this.apply(type.elem));
    if (type instanceof TPair) return new TPair(this.apply(type.fst), this.apply(type.snd));
    return type;
  }

  // Apply substitution to a scheme
  applyScheme(scheme) {
    // Don't substitute quantified variables
    const restricted = new Subst(new Map(this.map));
    for (const v of scheme.vars) restricted.map.delete(v);
    return new Scheme(scheme.vars, restricted.apply(scheme.type));
  }

  // Apply substitution to a type environment
  applyEnv(env) {
    const result = new TypeEnv(new Map(env.map));
    for (const [name, scheme] of result.map) {
      result.map.set(name, this.applyScheme(scheme));
    }
    return result;
  }

  // Compose: apply s2 first, then s1 (this)
  // compose(s2) means: first apply s2, then apply this
  compose(s2) {
    const result = new Map();
    // Apply this substitution to all of s2's mappings
    for (const [name, type] of s2.map) {
      result.set(name, this.apply(type));
    }
    // Add our own mappings (overriding if needed)
    for (const [name, type] of this.map) {
      if (!result.has(name)) result.set(name, type);
    }
    return new Subst(result);
  }
}

// ─── Type Environment ───
// Maps variable names to type schemes
class TypeEnv {
  constructor(map = new Map()) { this.map = map; }

  extend(name, scheme) {
    const m = new Map(this.map);
    m.set(name, scheme);
    return new TypeEnv(m);
  }

  lookup(name) { return this.map.get(name); }

  // Free type variables in the environment
  freeVars() {
    const result = new Set();
    for (const scheme of this.map.values()) {
      for (const v of freeTypeVars(scheme.type)) {
        if (!scheme.vars.includes(v)) result.add(v);
      }
    }
    return result;
  }
}

// ─── Free Type Variables ───
function freeTypeVars(type) {
  if (type instanceof TVar) return new Set([type.name]);
  if (type instanceof TCon) return new Set();
  if (type instanceof TFun) {
    const s = freeTypeVars(type.param);
    for (const v of freeTypeVars(type.result)) s.add(v);
    return s;
  }
  if (type instanceof TList) return freeTypeVars(type.elem);
  if (type instanceof TPair) {
    const s = freeTypeVars(type.fst);
    for (const v of freeTypeVars(type.snd)) s.add(v);
    return s;
  }
  return new Set();
}

// ─── Fresh Type Variables ───
let freshCounter = 0;
function resetFresh() { freshCounter = 0; }
function freshVar() {
  // Use t_ prefix to avoid collisions with scheme-quantified vars (a, b, c...)
  const name = 't_' + freshCounter;
  freshCounter++;
  return new TVar(name);
}

// ─── Occurs Check ───
function occurs(name, type) {
  if (type instanceof TVar) return type.name === name;
  if (type instanceof TCon) return false;
  if (type instanceof TFun) return occurs(name, type.param) || occurs(name, type.result);
  if (type instanceof TList) return occurs(name, type.elem);
  if (type instanceof TPair) return occurs(name, type.fst) || occurs(name, type.snd);
  return false;
}

// ─── Robinson Unification ───
function unify(t1, t2) {
  t1 = resolveType(t1);
  t2 = resolveType(t2);

  if (t1 instanceof TVar && t2 instanceof TVar && t1.name === t2.name) {
    return Subst.empty();
  }
  if (t1 instanceof TVar) {
    if (occurs(t1.name, t2)) throw new TypeError(`Infinite type: ${t1.name} occurs in ${t2}`);
    return Subst.single(t1.name, t2);
  }
  if (t2 instanceof TVar) {
    if (occurs(t2.name, t1)) throw new TypeError(`Infinite type: ${t2.name} occurs in ${t1}`);
    return Subst.single(t2.name, t1);
  }
  if (t1 instanceof TCon && t2 instanceof TCon) {
    if (t1.name === t2.name) return Subst.empty();
    throw new TypeError(`Cannot unify ${t1} with ${t2}`);
  }
  if (t1 instanceof TFun && t2 instanceof TFun) {
    const s1 = unify(t1.param, t2.param);
    const s2 = unify(s1.apply(t1.result), s1.apply(t2.result));
    return s2.compose(s1);
  }
  if (t1 instanceof TList && t2 instanceof TList) {
    return unify(t1.elem, t2.elem);
  }
  if (t1 instanceof TPair && t2 instanceof TPair) {
    const s1 = unify(t1.fst, t2.fst);
    const s2 = unify(s1.apply(t1.snd), s1.apply(t2.snd));
    return s2.compose(s1);
  }
  throw new TypeError(`Cannot unify ${t1} with ${t2}`);
}

function resolveType(t) {
  // No-op helper — substitution handles resolution
  return t;
}

// ─── Generalize & Instantiate ───
function generalize(env, type) {
  const envVars = env.freeVars();
  const typeVars = freeTypeVars(type);
  const quantified = [...typeVars].filter(v => !envVars.has(v));
  return new Scheme(quantified, type);
}

function instantiate(scheme) {
  const subst = new Map();
  for (const v of scheme.vars) {
    subst.set(v, freshVar());
  }
  const s = new Subst(subst);
  return s.apply(scheme.type);
}

// ─── AST Nodes ───
// { type: 'int', value }
// { type: 'bool', value }
// { type: 'string', value }
// { type: 'var', name }
// { type: 'lambda', param, body }
// { type: 'app', fn, arg }
// { type: 'let', name, value, body }
// { type: 'letrec', name, value, body }
// { type: 'if', cond, then, else }
// { type: 'binop', op, left, right }
// { type: 'unop', op, expr }
// { type: 'list', elems }
// { type: 'pair', fst, snd }

// ─── Parser ───
// Mini-ML syntax:
//   let id = \x -> x in id 42
//   let rec fact = \n -> if n == 0 then 1 else n * fact (n - 1) in fact 5
//   \x -> \y -> x + y
//   [1, 2, 3]
//   (1, true)

class Parser {
  constructor(src) {
    this.tokens = tokenize(src);
    this.pos = 0;
  }

  peek() { return this.pos < this.tokens.length ? this.tokens[this.pos] : null; }
  advance() { return this.tokens[this.pos++]; }
  expect(type, value) {
    const tok = this.advance();
    if (!tok || tok.type !== type || (value !== undefined && tok.value !== value)) {
      throw new Error(`Expected ${type}${value !== undefined ? ` '${value}'` : ''}, got ${tok ? `${tok.type} '${tok.value}'` : 'EOF'}`);
    }
    return tok;
  }

  parse() {
    const ast = this.parseExpr();
    if (this.pos < this.tokens.length) throw new Error(`Unexpected token: ${this.peek().value}`);
    return ast;
  }

  parseExpr() {
    return this.parseLet();
  }

  parseLet() {
    const tok = this.peek();
    if (tok && tok.type === 'kw' && tok.value === 'let') {
      this.advance();
      const rec = this.peek() && this.peek().type === 'kw' && this.peek().value === 'rec';
      if (rec) this.advance();
      const name = this.expect('id').value;
      this.expect('op', '=');
      const value = this.parseExpr();
      this.expect('kw', 'in');
      const body = this.parseExpr();
      return rec ? { type: 'letrec', name, value, body } : { type: 'let', name, value, body };
    }
    if (tok && tok.type === 'kw' && tok.value === 'if') {
      this.advance();
      const cond = this.parseExpr();
      this.expect('kw', 'then');
      const thenBranch = this.parseExpr();
      this.expect('kw', 'else');
      const elseBranch = this.parseExpr();
      return { type: 'if', cond, then: thenBranch, else: elseBranch };
    }
    if (tok && tok.type === 'op' && tok.value === '\\') {
      this.advance();
      const param = this.expect('id').value;
      this.expect('op', '->');
      const body = this.parseExpr();
      return { type: 'lambda', param, body };
    }
    return this.parseComparison();
  }

  parseComparison() {
    let left = this.parseAddSub();
    while (this.peek() && this.peek().type === 'op' && ['==', '!=', '<', '>', '<=', '>='].includes(this.peek().value)) {
      const op = this.advance().value;
      const right = this.parseAddSub();
      left = { type: 'binop', op, left, right };
    }
    return left;
  }

  parseAddSub() {
    let left = this.parseMulDiv();
    while (this.peek() && this.peek().type === 'op' && ['+', '-', '++'].includes(this.peek().value)) {
      const op = this.advance().value;
      const right = this.parseMulDiv();
      left = { type: 'binop', op, left, right };
    }
    return left;
  }

  parseMulDiv() {
    let left = this.parseUnary();
    while (this.peek() && this.peek().type === 'op' && ['*', '/', '%'].includes(this.peek().value)) {
      const op = this.advance().value;
      const right = this.parseUnary();
      left = { type: 'binop', op, left, right };
    }
    return left;
  }

  parseUnary() {
    if (this.peek() && this.peek().type === 'op' && this.peek().value === '-') {
      this.advance();
      const expr = this.parseApp();
      return { type: 'unop', op: 'neg', expr };
    }
    if (this.peek() && this.peek().type === 'kw' && this.peek().value === 'not') {
      this.advance();
      const expr = this.parseApp();
      return { type: 'unop', op: 'not', expr };
    }
    return this.parseApp();
  }

  parseApp() {
    let fn = this.parseAtom();
    while (this.peek() && !['op', 'kw'].includes(this.peek().type) || 
           (this.peek() && this.peek().type === 'op' && ['(', '[', '\\'].includes(this.peek().value))) {
      // Check if next token can start an atom
      if (!this.canStartAtom()) break;
      const arg = this.parseAtom();
      fn = { type: 'app', fn, arg };
    }
    return fn;
  }

  canStartAtom() {
    const tok = this.peek();
    if (!tok) return false;
    if (tok.type === 'int' || tok.type === 'bool' || tok.type === 'string' || tok.type === 'id') return true;
    if (tok.type === 'op' && (tok.value === '(' || tok.value === '[')) return true;
    return false;
  }

  parseAtom() {
    const tok = this.peek();
    if (!tok) throw new Error('Unexpected end of input');

    if (tok.type === 'int') { this.advance(); return { type: 'int', value: tok.value }; }
    if (tok.type === 'bool') { this.advance(); return { type: 'bool', value: tok.value }; }
    if (tok.type === 'string') { this.advance(); return { type: 'string', value: tok.value }; }
    if (tok.type === 'id') { this.advance(); return { type: 'var', name: tok.value }; }

    if (tok.type === 'op' && tok.value === '(') {
      this.advance();
      const expr = this.parseExpr();
      // Check for pair
      if (this.peek() && this.peek().type === 'op' && this.peek().value === ',') {
        this.advance();
        const snd = this.parseExpr();
        this.expect('op', ')');
        return { type: 'pair', fst: expr, snd };
      }
      this.expect('op', ')');
      return expr;
    }

    if (tok.type === 'op' && tok.value === '[') {
      this.advance();
      const elems = [];
      if (!(this.peek() && this.peek().type === 'op' && this.peek().value === ']')) {
        elems.push(this.parseExpr());
        while (this.peek() && this.peek().type === 'op' && this.peek().value === ',') {
          this.advance();
          elems.push(this.parseExpr());
        }
      }
      this.expect('op', ']');
      return { type: 'list', elems };
    }

    throw new Error(`Unexpected token: ${tok.type} '${tok.value}'`);
  }
}

function tokenize(src) {
  const tokens = [];
  let i = 0;
  const keywords = new Set(['let', 'rec', 'in', 'if', 'then', 'else', 'true', 'false', 'not']);
  const ops = ['\\', '->', '==', '!=', '<=', '>=', '++', '<', '>', '+', '-', '*', '/', '%',
    '(', ')', '[', ']', ',', '=', ':'];

  while (i < src.length) {
    if (/\s/.test(src[i])) { i++; continue; }
    if (src[i] === '-' && src[i + 1] === '-') { while (i < src.length && src[i] !== '\n') i++; continue; }

    // Numbers
    if (/\d/.test(src[i])) {
      let num = '';
      while (i < src.length && /\d/.test(src[i])) num += src[i++];
      tokens.push({ type: 'int', value: parseInt(num, 10) });
      continue;
    }

    // Strings
    if (src[i] === '"') {
      i++;
      let str = '';
      while (i < src.length && src[i] !== '"') {
        if (src[i] === '\\') { i++; str += src[i] === 'n' ? '\n' : src[i] === 't' ? '\t' : src[i]; }
        else str += src[i];
        i++;
      }
      i++; // closing "
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(src[i])) {
      let id = '';
      while (i < src.length && /[a-zA-Z0-9_']/.test(src[i])) id += src[i++];
      if (id === 'true' || id === 'false') {
        tokens.push({ type: 'bool', value: id === 'true' });
      } else if (keywords.has(id)) {
        tokens.push({ type: 'kw', value: id });
      } else {
        tokens.push({ type: 'id', value: id });
      }
      continue;
    }

    // Operators
    let matched = false;
    for (const op of ops) {
      if (src.slice(i, i + op.length) === op) {
        tokens.push({ type: 'op', value: op });
        i += op.length;
        matched = true;
        break;
      }
    }
    if (matched) continue;

    throw new Error(`Unexpected character: '${src[i]}' at position ${i}`);
  }
  return tokens;
}

// ─── Algorithm W ───
function infer(expr, env) {
  if (!env) env = defaultEnv();
  resetFresh();
  const [subst, type] = algorithmW(env, expr);
  return subst.apply(type);
}

function algorithmW(env, expr) {
  switch (expr.type) {
    case 'int': return [Subst.empty(), tInt];
    case 'bool': return [Subst.empty(), tBool];
    case 'string': return [Subst.empty(), tString];

    case 'var': {
      const scheme = env.lookup(expr.name);
      if (!scheme) throw new TypeError(`Unbound variable: ${expr.name}`);
      return [Subst.empty(), instantiate(scheme)];
    }

    case 'lambda': {
      const tv = freshVar();
      const newEnv = env.extend(expr.param, new Scheme([], tv));
      const [s1, t1] = algorithmW(newEnv, expr.body);
      return [s1, new TFun(s1.apply(tv), t1)];
    }

    case 'app': {
      const tv = freshVar();
      const [s1, t1] = algorithmW(env, expr.fn);
      const [s2, t2] = algorithmW(s1.applyEnv(env), expr.arg);
      const s3 = unify(s2.apply(t1), new TFun(t2, tv));
      return [s3.compose(s2).compose(s1), s3.apply(tv)];
    }

    case 'let': {
      const [s1, t1] = algorithmW(env, expr.value);
      const newEnv = s1.applyEnv(env);
      const scheme = generalize(newEnv, t1);
      const [s2, t2] = algorithmW(newEnv.extend(expr.name, scheme), expr.body);
      return [s2.compose(s1), t2];
    }

    case 'letrec': {
      const tv = freshVar();
      const recEnv = env.extend(expr.name, new Scheme([], tv));
      const [s1, t1] = algorithmW(recEnv, expr.value);
      const s2 = unify(s1.apply(tv), t1);
      const combinedSubst = s2.compose(s1);
      const finalEnv = combinedSubst.applyEnv(env);
      const scheme = generalize(finalEnv, combinedSubst.apply(tv));
      const [s3, t2] = algorithmW(finalEnv.extend(expr.name, scheme), expr.body);
      return [s3.compose(combinedSubst), t2];
    }

    case 'if': {
      const [s1, t1] = algorithmW(env, expr.cond);
      const s1b = unify(t1, tBool);
      const env2 = s1b.compose(s1).applyEnv(env);
      const [s2, t2] = algorithmW(env2, expr.then);
      const env3 = s2.applyEnv(env2);
      const [s3, t3] = algorithmW(env3, expr.else);
      const s4 = unify(s3.apply(t2), t3);
      return [s4.compose(s3).compose(s2).compose(s1b).compose(s1), s4.apply(t3)];
    }

    case 'binop': {
      const [s1, t1] = algorithmW(env, expr.left);
      const [s2, t2] = algorithmW(s1.applyEnv(env), expr.right);
      const s = s2.compose(s1);

      if (['+', '-', '*', '/', '%'].includes(expr.op)) {
        const s3 = unify(s.apply(t1), tInt);
        const s4 = unify(s3.apply(t2), tInt);
        return [s4.compose(s3).compose(s), tInt];
      }
      if (['==', '!=', '<', '>', '<=', '>='].includes(expr.op)) {
        const s3 = unify(s.apply(t1), s.apply(t2));
        return [s3.compose(s), tBool];
      }
      if (expr.op === '++') {
        // String concatenation
        const s3 = unify(s.apply(t1), tString);
        const s4 = unify(s3.apply(t2), tString);
        return [s4.compose(s3).compose(s), tString];
      }
      throw new TypeError(`Unknown operator: ${expr.op}`);
    }

    case 'unop': {
      const [s1, t1] = algorithmW(env, expr.expr);
      if (expr.op === 'neg') {
        const s2 = unify(t1, tInt);
        return [s2.compose(s1), tInt];
      }
      if (expr.op === 'not') {
        const s2 = unify(t1, tBool);
        return [s2.compose(s1), tBool];
      }
      throw new TypeError(`Unknown unary operator: ${expr.op}`);
    }

    case 'list': {
      if (expr.elems.length === 0) {
        return [Subst.empty(), new TList(freshVar())];
      }
      let s = Subst.empty();
      let elemType = null;
      for (const elem of expr.elems) {
        const [si, ti] = algorithmW(s.applyEnv(env), elem);
        s = si.compose(s);
        if (elemType) {
          const su = unify(s.apply(elemType), ti);
          s = su.compose(s);
          elemType = su.apply(ti);
        } else {
          elemType = ti;
        }
      }
      return [s, new TList(s.apply(elemType))];
    }

    case 'pair': {
      const [s1, t1] = algorithmW(env, expr.fst);
      const [s2, t2] = algorithmW(s1.applyEnv(env), expr.snd);
      return [s2.compose(s1), new TPair(s2.apply(t1), t2)];
    }

    default:
      throw new TypeError(`Unknown expression type: ${expr.type}`);
  }
}

// ─── Default Environment ───
function defaultEnv() {
  const env = new TypeEnv();
  const a = new TVar('_a');
  return env
    .extend('head', new Scheme(['_a'], new TFun(new TList(a), a)))
    .extend('tail', new Scheme(['_a'], new TFun(new TList(a), new TList(a))))
    .extend('cons', new Scheme(['_a'], new TFun(a, new TFun(new TList(a), new TList(a)))))
    .extend('null', new Scheme(['_a'], new TFun(new TList(a), tBool)))
    .extend('length', new Scheme(['_a'], new TFun(new TList(a), tInt)))
    .extend('fst', new Scheme(['_a', '_b'], new TFun(new TPair(a, new TVar('_b')), a)))
    .extend('snd', new Scheme(['_a', '_b'], new TFun(new TPair(new TVar('_a'), new TVar('_b')), new TVar('_b'))))
    ;
}

// ─── Convenience: parse + infer ───
function typeOf(src) {
  const parser = new Parser(src);
  const ast = parser.parse();
  const type = infer(ast);
  return type.toString();
}

export { typeOf, TVar, TCon, TFun, TList, TPair, tInt, tBool, tString, tUnit,
         Scheme, Subst, TypeEnv, unify, generalize, instantiate,
         freeTypeVars, occurs, freshVar, resetFresh, infer, Parser,
         resolveType, algorithmW, defaultEnv, tokenize };
