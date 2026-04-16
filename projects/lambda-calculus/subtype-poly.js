/**
 * Subtype Polymorphism: Width and Depth Subtyping
 * 
 * Record/object subtyping:
 * - Width: {x:Int, y:Int, z:Int} <: {x:Int, y:Int}  (more fields is subtype)
 * - Depth: {x:Nat} <: {x:Int}  (if Nat <: Int)
 * - Function: contravariant param, covariant return
 * 
 * Method dispatch: virtual method tables
 */

class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TRecord {
  constructor(fields) { this.tag = 'TRecord'; this.fields = fields; } // Map<name, type>
  toString() { return `{${[...this.fields].map(([k,v]) => `${k}: ${v}`).join(', ')}}`; }
}
class TFun {
  constructor(param, ret) { this.tag = 'TFun'; this.param = param; this.ret = ret; }
  toString() { return `(${this.param} → ${this.ret})`; }
}

const tInt = new TBase('Int');
const tNat = new TBase('Nat');  // Natural numbers ⊂ Int
const tStr = new TBase('Str');
const tBool = new TBase('Bool');
const tTop = new TBase('Top');
const tBot = new TBase('Bot');

// Base type subtyping relation
const baseSubtypes = new Map([
  ['Nat', ['Int']],    // Nat <: Int
  ['Int', ['Top']],
  ['Str', ['Top']],
  ['Bool', ['Top']],
  ['Bot', ['Nat', 'Int', 'Str', 'Bool', 'Top']],
]);

function isBaseSubtype(a, b) {
  if (a === b) return true;
  const parents = baseSubtypes.get(a);
  if (!parents) return false;
  if (parents.includes(b)) return true;
  // Transitive closure
  return parents.some(p => isBaseSubtype(p, b));
}

function isSubtype(t1, t2) {
  if (t1.tag === 'TBase' && t2.tag === 'TBase') return isBaseSubtype(t1.name, t2.name);
  
  if (t1.tag === 'TRecord' && t2.tag === 'TRecord') {
    // Width + depth subtyping
    for (const [key, t2Type] of t2.fields) {
      const t1Type = t1.fields.get(key);
      if (!t1Type) return false; // Missing field
      if (!isSubtype(t1Type, t2Type)) return false; // Depth check
    }
    return true; // All t2 fields present in t1 with compatible types
  }
  
  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    // Contravariant param, covariant return
    return isSubtype(t2.param, t1.param) && isSubtype(t1.ret, t2.ret);
  }
  
  return false;
}

// ============================================================
// Virtual Method Table
// ============================================================

class VTable {
  constructor(methods) { this.methods = methods; } // Map<name, function>
  
  dispatch(name, self, ...args) {
    const method = this.methods.get(name);
    if (!method) throw new Error(`No method: ${name}`);
    return method(self, ...args);
  }
}

class Object_ {
  constructor(fields, vtable) { this.fields = fields; this.vtable = vtable; }
  
  call(method, ...args) {
    return this.vtable.dispatch(method, this, ...args);
  }
  
  get(field) { return this.fields.get(field); }
}

// ============================================================
// Example: shape hierarchy
// ============================================================

const shapeVtable = new VTable(new Map([
  ['area', (self) => 0],
  ['describe', (self) => 'shape'],
]));

const circleVtable = new VTable(new Map([
  ['area', (self) => Math.PI * self.get('radius') ** 2],
  ['describe', (self) => `circle(r=${self.get('radius')})`],
  ['circumference', (self) => 2 * Math.PI * self.get('radius')],
]));

const rectVtable = new VTable(new Map([
  ['area', (self) => self.get('width') * self.get('height')],
  ['describe', (self) => `rect(${self.get('width')}×${self.get('height')})`],
]));

function circle(r) {
  return new Object_(new Map([['radius', r]]), circleVtable);
}

function rect(w, h) {
  return new Object_(new Map([['width', w], ['height', h]]), rectVtable);
}

export {
  TBase, TRecord, TFun,
  tInt, tNat, tStr, tBool, tTop, tBot,
  isSubtype, isBaseSubtype,
  VTable, Object_, circle, rect,
  shapeVtable, circleVtable, rectVtable
};
