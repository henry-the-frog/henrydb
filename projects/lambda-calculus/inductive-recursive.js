/**
 * Inductive-Recursive Types
 * 
 * Types and functions defined simultaneously:
 * - The TYPE depends on the FUNCTION (recursive)
 * - The FUNCTION is defined by recursion on the TYPE (inductive)
 * 
 * Example: Universe pattern
 *   data U : Set where     -- TYPE
 *     BOOL : U
 *     NAT  : U  
 *     PI   : (a : U) → (El a → U) → U
 *   
 *   El : U → Set           -- FUNCTION (defined simultaneously)
 *   El BOOL = Bool
 *   El NAT  = Nat
 *   El (PI a b) = (x : El a) → El (b x)
 */

// Universe codes
class UBool { constructor() { this.tag = 'UBool'; } toString() { return 'Bool'; } }
class UNat { constructor() { this.tag = 'UNat'; } toString() { return 'Nat'; } }
class UStr { constructor() { this.tag = 'UStr'; } toString() { return 'Str'; } }
class UPi { constructor(domain, codomain) { this.tag = 'UPi'; this.domain = domain; this.codomain = codomain; } toString() { return `Π(${this.domain}).?`; } }
class UList { constructor(elem) { this.tag = 'UList'; this.elem = elem; } toString() { return `[${this.elem}]`; } }
class UPair { constructor(fst, snd) { this.tag = 'UPair'; this.fst = fst; this.snd = snd; } toString() { return `(${this.fst} × ${this.snd})`; } }

// Decoding function: U → Set (maps codes to actual types)
function El(code) {
  switch (code.tag) {
    case 'UBool': return { type: 'Bool', check: v => typeof v === 'boolean' };
    case 'UNat': return { type: 'Nat', check: v => typeof v === 'number' && v >= 0 && Number.isInteger(v) };
    case 'UStr': return { type: 'Str', check: v => typeof v === 'string' };
    case 'UList': {
      const inner = El(code.elem);
      return { type: `[${inner.type}]`, check: v => Array.isArray(v) && v.every(inner.check) };
    }
    case 'UPair': {
      const f = El(code.fst);
      const s = El(code.snd);
      return { type: `(${f.type} × ${s.type})`, check: v => Array.isArray(v) && v.length === 2 && f.check(v[0]) && s.check(v[1]) };
    }
    case 'UPi': {
      const dom = El(code.domain);
      return { type: `(${dom.type} → ?)`, check: v => typeof v === 'function' };
    }
    default: throw new Error(`Unknown universe code: ${code.tag}`);
  }
}

// Type-safe operations using the universe
function typeCheck(code, value) {
  const decoded = El(code);
  return { valid: decoded.check(value), type: decoded.type };
}

function genericEq(code, a, b) {
  switch (code.tag) {
    case 'UBool': case 'UNat': case 'UStr': return a === b;
    case 'UList': {
      if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
      return a.every((x, i) => genericEq(code.elem, x, b[i]));
    }
    case 'UPair': return genericEq(code.fst, a[0], b[0]) && genericEq(code.snd, a[1], b[1]);
    default: return false;
  }
}

function genericShow(code, value) {
  switch (code.tag) {
    case 'UBool': return String(value);
    case 'UNat': return String(value);
    case 'UStr': return `"${value}"`;
    case 'UList': return `[${value.map(v => genericShow(code.elem, v)).join(', ')}]`;
    case 'UPair': return `(${genericShow(code.fst, value[0])}, ${genericShow(code.snd, value[1])})`;
    default: return String(value);
  }
}

function genericSize(code, value) {
  switch (code.tag) {
    case 'UBool': case 'UNat': case 'UStr': return 1;
    case 'UList': return 1 + value.reduce((acc, v) => acc + genericSize(code.elem, v), 0);
    case 'UPair': return genericSize(code.fst, value[0]) + genericSize(code.snd, value[1]);
    default: return 1;
  }
}

export {
  UBool, UNat, UStr, UPi, UList, UPair,
  El, typeCheck, genericEq, genericShow, genericSize
};
