/**
 * Polymorphic Variants (OCaml-style)
 * 
 * Open sum types: constructors don't belong to a fixed type.
 * Any value tagged `A(int) can be part of any type containing `A(int).
 * 
 * vs Regular ADTs: type list = Nil | Cons(a, list) — CLOSED, fixed constructors
 * vs Poly variants: `Nil | `Cons(a, ...) — OPEN, extensible
 * 
 * Subtyping: [`A | `B] <: [`A | `B | `C]  (can add tags)
 * Matching: match on [`A | `B] can handle subset of a larger type
 * 
 * Used in OCaml for extensible error types, plugin systems.
 */

// ============================================================
// Types
// ============================================================

class TBase {
  constructor(name) { this.tag = 'TBase'; this.name = name; }
  toString() { return this.name; }
}

class TPolyVariant {
  constructor(tags, closed = true) {
    this.tag = 'TPolyVariant';
    this.tags = tags;    // Map<tagName, argType | null>
    this.closed = closed; // true = exact, false = at least these tags (upper bound)
  }
  toString() {
    const parts = [...this.tags].map(([k, v]) => v ? `\`${k}(${v})` : `\`${k}`);
    return `[${this.closed ? '' : '> '}${parts.join(' | ')}]`;
  }
}

const tInt = new TBase('Int');
const tBool = new TBase('Bool');
const tStr = new TBase('Str');

// ============================================================
// Values
// ============================================================

class VTag {
  constructor(name, arg = null) { this.tag = 'VTag'; this.name = name; this.arg = arg; }
  toString() { return this.arg !== null ? `\`${this.name}(${this.arg})` : `\`${this.name}`; }
}

// ============================================================
// Subtyping
// ============================================================

function isSubtype(t1, t2) {
  if (t1.tag !== 'TPolyVariant' || t2.tag !== 'TPolyVariant') return false;
  
  // t1 <: t2 if every tag in t1 is also in t2 with compatible arg types
  for (const [tag, argType] of t1.tags) {
    if (!t2.tags.has(tag)) return false;
    const t2Arg = t2.tags.get(tag);
    if (argType && t2Arg) {
      if (!typeEquals(argType, t2Arg)) return false;
    }
  }
  return true;
}

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'TBase') return a.name === b.name;
  if (a.tag === 'TPolyVariant') {
    if (a.tags.size !== b.tags.size) return false;
    for (const [tag, argType] of a.tags) {
      if (!b.tags.has(tag)) return false;
      if (argType && !typeEquals(argType, b.tags.get(tag))) return false;
    }
    return true;
  }
  return false;
}

// ============================================================
// Type operations
// ============================================================

function unionVariants(t1, t2) {
  const tags = new Map(t1.tags);
  for (const [tag, argType] of t2.tags) {
    if (!tags.has(tag)) tags.set(tag, argType);
  }
  return new TPolyVariant(tags, false);
}

function intersectVariants(t1, t2) {
  const tags = new Map();
  for (const [tag, argType] of t1.tags) {
    if (t2.tags.has(tag)) tags.set(tag, argType);
  }
  return new TPolyVariant(tags, true);
}

// ============================================================
// Pattern matching
// ============================================================

function matchVariant(value, cases) {
  // cases: [{tag, param, body}]
  for (const c of cases) {
    if (c.tag === value.name) {
      return { matched: true, param: c.param, arg: value.arg, body: c.body };
    }
  }
  return { matched: false };
}

// ============================================================
// Exhaustiveness check
// ============================================================

function checkExhaustive(type, cases) {
  if (type.tag !== 'TPolyVariant') return { exhaustive: false, missing: [] };
  
  const caseTagSet = new Set(cases.map(c => c.tag));
  const missing = [];
  
  for (const [tag] of type.tags) {
    if (!caseTagSet.has(tag)) missing.push(tag);
  }
  
  return { exhaustive: missing.length === 0, missing };
}

export {
  TBase, TPolyVariant, tInt, tBool, tStr,
  VTag,
  isSubtype, typeEquals,
  unionVariants, intersectVariants,
  matchVariant, checkExhaustive
};
