/**
 * Refinement Types
 * 
 * Types refined with logical predicates:
 *   {x:T | P(x)} — values of type T that satisfy predicate P
 * 
 * Examples:
 *   {x:Int | x > 0}     — positive integers
 *   {x:Int | x >= 0}    — natural numbers
 *   {x:Int | x % 2 == 0} — even integers
 *   {x:[Int] | len(x) > 0} — non-empty lists
 * 
 * Subtyping: {x:T | P(x)} <: {x:T | Q(x)} iff P(x) ⇒ Q(x)
 * This connects to SMT solving: the constraint P ⇒ Q is checked.
 * 
 * Based on:
 * - Liquid Haskell (Vazou et al.)
 * - Refined Types for ML (Freeman & Pfenning)
 */

// ============================================================
// Types
// ============================================================

class RBase {
  constructor(name) { this.tag = 'RBase'; this.name = name; }
  toString() { return this.name; }
}

class RRefined {
  constructor(varName, baseType, predicate) {
    this.tag = 'RRefined';
    this.varName = varName;   // Binding variable name
    this.baseType = baseType; // Base type
    this.predicate = predicate; // Predicate AST
  }
  toString() {
    return `{${this.varName}:${this.baseType} | ${formatPred(this.predicate)}}`;
  }
}

class RFun {
  constructor(param, paramType, retType) {
    this.tag = 'RFun';
    this.param = param;
    this.paramType = paramType;
    this.retType = retType; // May reference param
  }
  toString() {
    return `(${this.param}:${this.paramType}) → ${this.retType}`;
  }
}

class RList {
  constructor(elemType) { this.tag = 'RList'; this.elemType = elemType; }
  toString() { return `[${this.elemType}]`; }
}

// Base types
const rInt = new RBase('Int');
const rBool = new RBase('Bool');
const rStr = new RBase('Str');
const rUnit = new RBase('Unit');

// ============================================================
// Predicates
// ============================================================

class PVar { constructor(name) { this.tag = 'PVar'; this.name = name; } }
class PNum { constructor(n) { this.tag = 'PNum'; this.n = n; } }
class PBool { constructor(v) { this.tag = 'PBool'; this.v = v; } }
class PBinOp { constructor(op, left, right) { this.tag = 'PBinOp'; this.op = op; this.left = left; this.right = right; } }
class PNot { constructor(inner) { this.tag = 'PNot'; this.inner = inner; } }
class PCall { constructor(fn, args) { this.tag = 'PCall'; this.fn = fn; this.args = args; } }

function formatPred(p) {
  if (!p) return 'true';
  switch (p.tag) {
    case 'PVar': return p.name;
    case 'PNum': return `${p.n}`;
    case 'PBool': return `${p.v}`;
    case 'PBinOp': return `${formatPred(p.left)} ${p.op} ${formatPred(p.right)}`;
    case 'PNot': return `¬(${formatPred(p.inner)})`;
    case 'PCall': return `${p.fn}(${p.args.map(formatPred).join(', ')})`;
    default: return '?';
  }
}

// Predicate constructors
function pvar(name) { return new PVar(name); }
function pnum(n) { return new PNum(n); }
function pbool(v) { return new PBool(v); }
function pgt(a, b) { return new PBinOp('>', a, b); }
function pge(a, b) { return new PBinOp('>=', a, b); }
function plt(a, b) { return new PBinOp('<', a, b); }
function ple(a, b) { return new PBinOp('<=', a, b); }
function peq(a, b) { return new PBinOp('==', a, b); }
function pand(a, b) { return new PBinOp('&&', a, b); }
function por(a, b) { return new PBinOp('||', a, b); }
function pmod(a, b) { return new PBinOp('%', a, b); }
function padd(a, b) { return new PBinOp('+', a, b); }
function pnot(p) { return new PNot(p); }

// ============================================================
// Common Refined Types
// ============================================================

function posInt() { return new RRefined('x', rInt, pgt(pvar('x'), pnum(0))); }
function natType() { return new RRefined('x', rInt, pge(pvar('x'), pnum(0))); }
function evenInt() { return new RRefined('x', rInt, peq(pmod(pvar('x'), pnum(2)), pnum(0))); }
function boundedInt(lo, hi) { return new RRefined('x', rInt, pand(pge(pvar('x'), pnum(lo)), ple(pvar('x'), pnum(hi)))); }
function nonEmpty(elemType) { return new RRefined('xs', new RList(elemType), pgt(new PCall('len', [pvar('xs')]), pnum(0))); }

// ============================================================
// Subtyping
// ============================================================

/**
 * Check if T₁ <: T₂ (T₁ is a subtype of T₂)
 * For refined types: {x:T | P(x)} <: {x:T | Q(x)} iff P(x) ⇒ Q(x)
 * 
 * @returns {object} { isSubtype, reason }
 */
function isSubtype(t1, t2) {
  // Same base types are subtypes of each other
  if (t1.tag === 'RBase' && t2.tag === 'RBase') {
    return { isSubtype: t1.name === t2.name, reason: t1.name === t2.name ? 'same base type' : `${t1.name} ≠ ${t2.name}` };
  }
  
  // Refined <: Base (drop refinement)
  if (t1.tag === 'RRefined' && t2.tag === 'RBase') {
    const baseMatch = t1.baseType.name === t2.name;
    return { isSubtype: baseMatch, reason: baseMatch ? 'refined subtype of base' : 'base type mismatch' };
  }
  
  // Base <: Refined (must check predicate is trivially true)
  if (t1.tag === 'RBase' && t2.tag === 'RRefined') {
    // Only if the predicate is trivially satisfied
    const trivial = isTrivialPredicate(t2.predicate);
    return { isSubtype: trivial, reason: trivial ? 'trivial predicate' : 'predicate may not hold for all base values' };
  }
  
  // Refined <: Refined
  if (t1.tag === 'RRefined' && t2.tag === 'RRefined') {
    if (t1.baseType.name !== t2.baseType.name) {
      return { isSubtype: false, reason: 'different base types' };
    }
    // P₁(x) ⇒ P₂(x)?
    const implies = checkImplication(t1.predicate, t2.predicate, t1.varName, t2.varName);
    return implies;
  }
  
  // Function subtyping (contravariant in param, covariant in return)
  if (t1.tag === 'RFun' && t2.tag === 'RFun') {
    const paramSub = isSubtype(t2.paramType, t1.paramType); // Contravariant
    const retSub = isSubtype(t1.retType, t2.retType);       // Covariant
    const ok = paramSub.isSubtype && retSub.isSubtype;
    return { isSubtype: ok, reason: ok ? 'function subtyping' : `param: ${paramSub.reason}, ret: ${retSub.reason}` };
  }
  
  return { isSubtype: false, reason: 'incompatible types' };
}

// Simple implication checker (not a full SMT solver)
function checkImplication(p1, p2, var1, var2) {
  // Rename var2 to var1 for comparison
  const p2Renamed = renamePredVar(p2, var2, var1);
  
  // Syntactic equality
  if (predEquals(p1, p2Renamed)) {
    return { isSubtype: true, reason: 'predicates are syntactically equal' };
  }
  
  // P ⇒ P (reflexive)
  if (predEquals(p1, p2Renamed)) {
    return { isSubtype: true, reason: 'same predicate' };
  }
  
  // x > n₁ ⇒ x > n₂ if n₁ ≥ n₂ (strengthening)
  if (p1.tag === 'PBinOp' && p2Renamed.tag === 'PBinOp') {
    const l1 = p1.left, r1 = p1.right;
    const l2 = p2Renamed.left, r2 = p2Renamed.right;
    
    if (predEquals(l1, l2) && r1.tag === 'PNum' && r2.tag === 'PNum') {
      // x > n₁ ⇒ x > n₂ if n₁ ≥ n₂
      if (p1.op === '>' && p2Renamed.op === '>') {
        return { isSubtype: r1.n >= r2.n, reason: r1.n >= r2.n ? `${r1.n} ≥ ${r2.n}` : `${r1.n} < ${r2.n}` };
      }
      if (p1.op === '>=' && p2Renamed.op === '>=') {
        return { isSubtype: r1.n >= r2.n, reason: r1.n >= r2.n ? `${r1.n} ≥ ${r2.n}` : `${r1.n} < ${r2.n}` };
      }
      // x > n ⇒ x >= n (always)
      if (p1.op === '>' && p2Renamed.op === '>=') {
        return { isSubtype: r1.n >= r2.n, reason: '> implies >=' };
      }
      // x >= n ⇒ x > n-1
      if (p1.op === '>=' && p2Renamed.op === '>') {
        return { isSubtype: r1.n > r2.n, reason: r1.n > r2.n ? '>= with higher bound implies >' : 'bound too low' };
      }
      // x < n₁ ⇒ x < n₂ if n₁ ≤ n₂
      if (p1.op === '<' && p2Renamed.op === '<') {
        return { isSubtype: r1.n <= r2.n, reason: r1.n <= r2.n ? `${r1.n} ≤ ${r2.n}` : `bound too high` };
      }
      if (p1.op === '<=' && p2Renamed.op === '<=') {
        return { isSubtype: r1.n <= r2.n, reason: r1.n <= r2.n ? `${r1.n} ≤ ${r2.n}` : `bound too high` };
      }
    }
    
    // P₁ && P₂ ⇒ P₁ (weakening)
    if (p1.op === '&&') {
      if (predEquals(p1.left, p2Renamed) || predEquals(p1.right, p2Renamed)) {
        return { isSubtype: true, reason: 'conjunction implies conjunct' };
      }
    }
  }
  
  // P ⇒ true (anything implies true)
  if (p2Renamed.tag === 'PBool' && p2Renamed.v === true) {
    return { isSubtype: true, reason: 'anything implies true' };
  }
  
  // Default: can't prove (conservative)
  return { isSubtype: false, reason: `cannot prove ${formatPred(p1)} ⇒ ${formatPred(p2Renamed)}` };
}

function isTrivialPredicate(p) {
  return p.tag === 'PBool' && p.v === true;
}

function predEquals(a, b) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'PVar': return a.name === b.name;
    case 'PNum': return a.n === b.n;
    case 'PBool': return a.v === b.v;
    case 'PBinOp': return a.op === b.op && predEquals(a.left, b.left) && predEquals(a.right, b.right);
    case 'PNot': return predEquals(a.inner, b.inner);
    case 'PCall': return a.fn === b.fn && a.args.length === b.args.length && a.args.every((x, i) => predEquals(x, b.args[i]));
    default: return false;
  }
}

function renamePredVar(p, from, to) {
  if (from === to) return p;
  switch (p.tag) {
    case 'PVar': return p.name === from ? new PVar(to) : p;
    case 'PNum': case 'PBool': return p;
    case 'PBinOp': return new PBinOp(p.op, renamePredVar(p.left, from, to), renamePredVar(p.right, from, to));
    case 'PNot': return new PNot(renamePredVar(p.inner, from, to));
    case 'PCall': return new PCall(p.fn, p.args.map(a => renamePredVar(a, from, to)));
    default: return p;
  }
}

// ============================================================
// Value Checking
// ============================================================

function checkValue(value, type) {
  if (type.tag === 'RBase') {
    switch (type.name) {
      case 'Int': return typeof value === 'number' && Number.isInteger(value);
      case 'Bool': return typeof value === 'boolean';
      case 'Str': return typeof value === 'string';
      case 'Unit': return value === null || value === undefined;
      default: return false;
    }
  }
  
  if (type.tag === 'RRefined') {
    if (!checkValue(value, type.baseType)) return false;
    return evalPredicate(type.predicate, { [type.varName]: value });
  }
  
  return false;
}

function evalPredicate(pred, env) {
  switch (pred.tag) {
    case 'PVar': return env[pred.name];
    case 'PNum': return pred.n;
    case 'PBool': return pred.v;
    case 'PBinOp': {
      const l = evalPredicate(pred.left, env);
      const r = evalPredicate(pred.right, env);
      switch (pred.op) {
        case '+': return l + r;
        case '-': return l - r;
        case '*': return l * r;
        case '/': return Math.floor(l / r);
        case '%': return l % r;
        case '>': return l > r;
        case '>=': return l >= r;
        case '<': return l < r;
        case '<=': return l <= r;
        case '==': return l === r;
        case '!=': return l !== r;
        case '&&': return l && r;
        case '||': return l || r;
        default: return false;
      }
    }
    case 'PNot': return !evalPredicate(pred.inner, env);
    case 'PCall': {
      const args = pred.args.map(a => evalPredicate(a, env));
      switch (pred.fn) {
        case 'len': return Array.isArray(args[0]) ? args[0].length : 0;
        default: return 0;
      }
    }
    default: return false;
  }
}

// ============================================================
// Exports
// ============================================================

export {
  RBase, RRefined, RFun, RList,
  rInt, rBool, rStr, rUnit,
  PVar, PNum, PBool, PBinOp, PNot, PCall,
  pvar, pnum, pbool, pgt, pge, plt, ple, peq, pand, por, pmod, padd, pnot,
  formatPred,
  posInt, natType, evenInt, boundedInt, nonEmpty,
  isSubtype, checkImplication,
  checkValue, evalPredicate
};
