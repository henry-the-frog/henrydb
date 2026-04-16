/**
 * Intersection and Union Types
 * 
 * Intersection: T₁ & T₂ — value has both types simultaneously
 * Union: T₁ | T₂ — value has one type or the other
 * 
 * Used in TypeScript, Scala, CDuce, and flow-sensitive type systems.
 * 
 * Subtyping rules:
 *   T₁ & T₂ <: T₁       (intersection eliminates left)
 *   T₁ & T₂ <: T₂       (intersection eliminates right)
 *   T <: T₁ → T <: T₁ | T₂  (union introduces)
 *   T₁ | T₂ <: T → T₁ <: T ∧ T₂ <: T  (union eliminates)
 *   
 * For functions:
 *   (A → B) & (A → C) <: A → (B & C)  (intersection distributes)
 */

// ============================================================
// Types
// ============================================================

class TBase {
  constructor(name) { this.tag = 'TBase'; this.name = name; }
  toString() { return this.name; }
}

class TIntersection {
  constructor(left, right) { this.tag = 'TIntersection'; this.left = left; this.right = right; }
  toString() { return `(${this.left} & ${this.right})`; }
}

class TUnion {
  constructor(left, right) { this.tag = 'TUnion'; this.left = left; this.right = right; }
  toString() { return `(${this.left} | ${this.right})`; }
}

class TFun {
  constructor(param, ret) { this.tag = 'TFun'; this.param = param; this.ret = ret; }
  toString() { return `(${this.param} → ${this.ret})`; }
}

class TRecord {
  constructor(fields) { this.tag = 'TRecord'; this.fields = fields; } // Map<string, Type>
  toString() { return `{${[...this.fields].map(([k,v]) => `${k}: ${v}`).join(', ')}}`; }
}

class TTop {
  constructor() { this.tag = 'TTop'; }
  toString() { return '⊤'; }
}

class TBottom {
  constructor() { this.tag = 'TBottom'; }
  toString() { return '⊥'; }
}

const tInt = new TBase('Int');
const tBool = new TBase('Bool');
const tStr = new TBase('Str');
const tTop = new TTop();
const tBottom = new TBottom();

// ============================================================
// Subtyping
// ============================================================

function isSubtype(t1, t2) {
  // Reflexivity
  if (typeEquals(t1, t2)) return true;
  
  // Top and Bottom
  if (t2.tag === 'TTop') return true;     // T <: ⊤ for all T
  if (t1.tag === 'TBottom') return true;   // ⊥ <: T for all T
  if (t1.tag === 'TTop') return false;     // ⊤ <: T only if T = ⊤
  if (t2.tag === 'TBottom') return false;  // T <: ⊥ only if T = ⊥
  
  // Intersection rules
  if (t1.tag === 'TIntersection') {
    // T₁ & T₂ <: T  if T₁ <: T or T₂ <: T
    if (isSubtype(t1.left, t2) || isSubtype(t1.right, t2)) return true;
  }
  if (t2.tag === 'TIntersection') {
    // T <: T₁ & T₂  if T <: T₁ and T <: T₂
    return isSubtype(t1, t2.left) && isSubtype(t1, t2.right);
  }
  
  // Union rules
  if (t1.tag === 'TUnion') {
    // T₁ | T₂ <: T  if T₁ <: T and T₂ <: T
    return isSubtype(t1.left, t2) && isSubtype(t1.right, t2);
  }
  if (t2.tag === 'TUnion') {
    // T <: T₁ | T₂  if T <: T₁ or T <: T₂
    if (isSubtype(t1, t2.left) || isSubtype(t1, t2.right)) return true;
  }
  
  // Function subtyping (contravariant param, covariant return)
  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    return isSubtype(t2.param, t1.param) && isSubtype(t1.ret, t2.ret);
  }
  
  // Record subtyping (width + depth)
  if (t1.tag === 'TRecord' && t2.tag === 'TRecord') {
    for (const [key, valType] of t2.fields) {
      const t1Val = t1.fields.get(key);
      if (!t1Val || !isSubtype(t1Val, valType)) return false;
    }
    return true;
  }
  
  return false;
}

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'TBase': return a.name === b.name;
    case 'TTop': case 'TBottom': return true;
    case 'TFun': return typeEquals(a.param, b.param) && typeEquals(a.ret, b.ret);
    case 'TIntersection': case 'TUnion':
      return typeEquals(a.left, b.left) && typeEquals(a.right, b.right);
    case 'TRecord': {
      if (a.fields.size !== b.fields.size) return false;
      for (const [k, v] of a.fields) {
        if (!b.fields.has(k) || !typeEquals(v, b.fields.get(k))) return false;
      }
      return true;
    }
    default: return false;
  }
}

// ============================================================
// Type Simplification
// ============================================================

function simplify(type) {
  // T & ⊤ = T
  if (type.tag === 'TIntersection') {
    const l = simplify(type.left);
    const r = simplify(type.right);
    if (l.tag === 'TTop') return r;
    if (r.tag === 'TTop') return l;
    if (l.tag === 'TBottom') return tBottom;
    if (r.tag === 'TBottom') return tBottom;
    if (typeEquals(l, r)) return l; // T & T = T
    return new TIntersection(l, r);
  }
  
  // T | ⊥ = T
  if (type.tag === 'TUnion') {
    const l = simplify(type.left);
    const r = simplify(type.right);
    if (l.tag === 'TBottom') return r;
    if (r.tag === 'TBottom') return l;
    if (l.tag === 'TTop') return tTop;
    if (r.tag === 'TTop') return tTop;
    if (typeEquals(l, r)) return l; // T | T = T
    return new TUnion(l, r);
  }
  
  return type;
}

// ============================================================
// Narrowing (flow-sensitive typing)
// ============================================================

function narrow(type, test) {
  // Given a type and a type test, return the narrowed type
  // narrow(Int | Str, isInt) = Int
  if (type.tag === 'TUnion') {
    if (isSubtype(type.left, test)) return type.left;
    if (isSubtype(type.right, test)) return type.right;
  }
  if (isSubtype(type, test)) return type;
  return tBottom; // impossible case
}

function widen(type, exclude) {
  // Given a type, remove a component
  // widen(Int | Str, Int) = Str
  if (type.tag === 'TUnion') {
    if (typeEquals(type.left, exclude)) return type.right;
    if (typeEquals(type.right, exclude)) return type.left;
  }
  return type;
}

// ============================================================
// Exports
// ============================================================

export {
  TBase, TIntersection, TUnion, TFun, TRecord, TTop, TBottom,
  tInt, tBool, tStr, tTop, tBottom,
  isSubtype, typeEquals, simplify, narrow, widen
};
