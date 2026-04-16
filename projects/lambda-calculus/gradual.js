/**
 * Gradual Type System
 * 
 * Smoothly transitions between static and dynamic typing.
 * Based on Siek & Taha (2006) "Gradual Typing for Functional Languages".
 * 
 * Key concepts:
 * - Dynamic type (?) is consistent with any type
 * - Consistent subtyping: T₁ ~ T₂ iff T₁ can flow to T₂ with casts
 * - Cast insertion: compile-time insertion of runtime type checks
 * - Blame tracking: when a cast fails, identify the responsible annotation
 * 
 * This models exactly how monkey-lang's type checker works:
 * - Unannotated code uses ? (dynamic type)
 * - Annotated code uses static types
 * - Mixed code gets automatic casts at boundaries
 */

// ============================================================
// Types
// ============================================================

class GDyn {
  // The dynamic type: consistent with everything
  constructor() { this.tag = 'GDyn'; }
  toString() { return '?'; }
}

class GInt {
  constructor() { this.tag = 'GInt'; }
  toString() { return 'Int'; }
}

class GBool {
  constructor() { this.tag = 'GBool'; }
  toString() { return 'Bool'; }
}

class GStr {
  constructor() { this.tag = 'GStr'; }
  toString() { return 'Str'; }
}

class GFun {
  constructor(param, ret) { this.tag = 'GFun'; this.param = param; this.ret = ret; }
  toString() { return `(${this.param} → ${this.ret})`; }
}

class GPair {
  constructor(fst, snd) { this.tag = 'GPair'; this.fst = fst; this.snd = snd; }
  toString() { return `(${this.fst} × ${this.snd})`; }
}

class GList {
  constructor(elem) { this.tag = 'GList'; this.elem = elem; }
  toString() { return `[${this.elem}]`; }
}

// Singleton instances
const dyn = new GDyn();
const gint = new GInt();
const gbool = new GBool();
const gstr = new GStr();

// ============================================================
// Consistency Relation
// ============================================================

/**
 * Type consistency: T₁ ~ T₂
 * The key relation in gradual typing.
 * ? is consistent with everything.
 * Otherwise, structural consistency.
 */
function consistent(t1, t2) {
  if (t1.tag === 'GDyn' || t2.tag === 'GDyn') return true;
  if (t1.tag !== t2.tag) return false;
  
  switch (t1.tag) {
    case 'GInt': case 'GBool': case 'GStr': return true;
    case 'GFun': return consistent(t1.param, t2.param) && consistent(t1.ret, t2.ret);
    case 'GPair': return consistent(t1.fst, t2.fst) && consistent(t1.snd, t2.snd);
    case 'GList': return consistent(t1.elem, t2.elem);
    default: return false;
  }
}

/**
 * Static type: contains no dynamic type
 */
function isStatic(t) {
  switch (t.tag) {
    case 'GDyn': return false;
    case 'GInt': case 'GBool': case 'GStr': return true;
    case 'GFun': return isStatic(t.param) && isStatic(t.ret);
    case 'GPair': return isStatic(t.fst) && isStatic(t.snd);
    case 'GList': return isStatic(t.elem);
    default: return false;
  }
}

/**
 * Ground type: a type constructor applied to ?
 */
function isGround(t) {
  switch (t.tag) {
    case 'GInt': case 'GBool': case 'GStr': return true;
    case 'GFun': return t.param.tag === 'GDyn' && t.ret.tag === 'GDyn';
    case 'GPair': return t.fst.tag === 'GDyn' && t.snd.tag === 'GDyn';
    case 'GList': return t.elem.tag === 'GDyn';
    default: return false;
  }
}

// ============================================================
// Cast Insertion
// ============================================================

class Cast {
  constructor(expr, fromType, toType, blame) {
    this.tag = 'Cast';
    this.expr = expr;
    this.fromType = fromType;
    this.toType = toType;
    this.blame = blame; // Label for blame tracking
  }
  toString() { return `⟨${this.toType} ⟸ ${this.fromType}⟩${this.expr}`; }
}

// ============================================================
// Blame Labels
// ============================================================

let blameCounter = 0;
function freshBlame() { return `blame_${blameCounter++}`; }
function resetBlame() { blameCounter = 0; }

// ============================================================
// Cast Evaluation (Runtime)
// ============================================================

class CastError extends Error {
  constructor(msg, blame) {
    super(msg);
    this.name = 'CastError';
    this.blame = blame;
  }
}

/**
 * Evaluate a cast at runtime.
 * @param {*} value - The value being cast
 * @param {Type} from - Source type
 * @param {Type} to - Target type
 * @param {string} blame - Blame label
 * @returns {*} The cast value (or throws CastError)
 */
function evalCast(value, from, to, blame) {
  // Identity cast: same type
  if (typeEquals(from, to)) return value;
  
  // Cast from ?
  if (from.tag === 'GDyn') {
    if (to.tag === 'GDyn') return value;
    
    // Check that the value matches the target type
    if (!runtimeTypeCheck(value, to)) {
      throw new CastError(
        `Cast failed: value ${value} cannot be cast from ? to ${to} (blame: ${blame})`,
        blame
      );
    }
    return value;
  }
  
  // Cast to ?
  if (to.tag === 'GDyn') return value; // Always succeeds (boxing)
  
  // Structural cast
  if (from.tag === 'GFun' && to.tag === 'GFun') {
    // Cast function: wrap with input/output casts
    // (T₁ → T₂) cast to (S₁ → S₂):
    // λx. cast(f(cast(x, S₁, T₁)), T₂, S₂)
    return {
      tag: 'CastFn',
      inner: value,
      paramCast: { from: to.param, to: from.param, blame: blame + '/arg' },
      retCast: { from: from.ret, to: to.ret, blame: blame + '/ret' },
    };
  }
  
  throw new CastError(`Cannot cast ${from} to ${to} (blame: ${blame})`, blame);
}

function runtimeTypeCheck(value, type) {
  switch (type.tag) {
    case 'GInt': return typeof value === 'number' && Number.isInteger(value);
    case 'GBool': return typeof value === 'boolean';
    case 'GStr': return typeof value === 'string';
    case 'GDyn': return true;
    case 'GFun': return typeof value === 'function' || (value && value.tag === 'CastFn');
    case 'GPair': return value && typeof value === 'object' && 'fst' in value && 'snd' in value;
    case 'GList': return Array.isArray(value);
    default: return false;
  }
}

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'GDyn': case 'GInt': case 'GBool': case 'GStr': return true;
    case 'GFun': return typeEquals(a.param, b.param) && typeEquals(a.ret, b.ret);
    case 'GPair': return typeEquals(a.fst, b.fst) && typeEquals(a.snd, b.snd);
    case 'GList': return typeEquals(a.elem, b.elem);
    default: return false;
  }
}

// ============================================================
// Meet / Join (for consistent types)
// ============================================================

/**
 * Type meet: greatest lower bound under consistency
 * meet(T, ?) = T
 * meet(?, T) = T
 * meet(T₁→T₂, S₁→S₂) = meet(T₁,S₁)→meet(T₂,S₂)
 */
function meet(t1, t2) {
  if (t1.tag === 'GDyn') return t2;
  if (t2.tag === 'GDyn') return t1;
  if (t1.tag !== t2.tag) return null; // inconsistent
  
  switch (t1.tag) {
    case 'GInt': case 'GBool': case 'GStr': return t1;
    case 'GFun': {
      const p = meet(t1.param, t2.param);
      const r = meet(t1.ret, t2.ret);
      if (!p || !r) return null;
      return new GFun(p, r);
    }
    case 'GPair': {
      const f = meet(t1.fst, t2.fst);
      const s = meet(t1.snd, t2.snd);
      if (!f || !s) return null;
      return new GPair(f, s);
    }
    case 'GList': {
      const e = meet(t1.elem, t2.elem);
      if (!e) return null;
      return new GList(e);
    }
    default: return null;
  }
}

/**
 * Type join: least upper bound under consistency
 * join(T, ?) = ?
 * join(?, T) = ?
 */
function join(t1, t2) {
  if (t1.tag === 'GDyn' || t2.tag === 'GDyn') return dyn;
  if (t1.tag !== t2.tag) return dyn; // can't join → go to ?
  
  switch (t1.tag) {
    case 'GInt': case 'GBool': case 'GStr': return t1;
    case 'GFun': return new GFun(join(t1.param, t2.param), join(t1.ret, t2.ret));
    case 'GPair': return new GPair(join(t1.fst, t2.fst), join(t1.snd, t2.snd));
    case 'GList': return new GList(join(t1.elem, t2.elem));
    default: return dyn;
  }
}

// ============================================================
// Exports
// ============================================================

export {
  GDyn, GInt, GBool, GStr, GFun, GPair, GList,
  dyn, gint, gbool, gstr,
  consistent, isStatic, isGround, typeEquals,
  Cast, evalCast, CastError, freshBlame, resetBlame, runtimeTypeCheck,
  meet, join
};
