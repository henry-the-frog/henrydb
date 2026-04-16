/**
 * Nominal Types: Identity by Name, Not Structure
 * 
 * Structural typing: {x: Int, y: Int} = {x: Int, y: Int} (same structure = same type)
 * Nominal typing: Point2D ≠ Vector2D even if both are {x: Int, y: Int} (different names = different types)
 * 
 * Used in: Java, C#, Rust (struct names matter), TypeScript (class nominal)
 */

class NominalType {
  constructor(name, structure) { this.name = name; this.structure = structure; }
  toString() { return this.name; }
}

function nominalEqual(t1, t2) { return t1.name === t2.name; }
function structuralEqual(t1, t2) { return JSON.stringify(t1.structure) === JSON.stringify(t2.structure); }

// Brand pattern: add a phantom brand field
class Branded {
  constructor(brand, value) { this._brand = brand; this.value = value; }
}

function brand(typeName) {
  return {
    make: value => new Branded(typeName, value),
    check: branded => branded instanceof Branded && branded._brand === typeName,
    unwrap: branded => {
      if (branded._brand !== typeName) throw new Error(`Expected ${typeName}, got ${branded._brand}`);
      return branded.value;
    }
  };
}

// Opaque types: hide the representation
class OpaqueType {
  constructor(name, repr, ops) { this.name = name; this.repr = repr; this.ops = ops; }
  create(value) { return { _type: this.name, _value: value }; }
  unwrap(wrapped) {
    if (wrapped._type !== this.name) throw new Error(`Type mismatch: expected ${this.name}`);
    return wrapped._value;
  }
}

// Newtype pattern: zero-cost nominal wrapper
function newtype(name) {
  const sym = Symbol(name);
  return {
    name,
    wrap: value => ({ [sym]: true, value }),
    unwrap: wrapped => { if (!wrapped[sym]) throw new Error(`Not a ${name}`); return wrapped.value; },
    is: value => value && value[sym] === true
  };
}

export { NominalType, nominalEqual, structuralEqual, brand, OpaqueType, newtype };
