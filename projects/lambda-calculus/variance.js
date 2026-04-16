/**
 * Variance Annotations
 * 
 * How subtyping of type constructors relates to subtyping of type arguments.
 * 
 * Given A <: B:
 * - Covariant (+):     F<A> <: F<B>      (e.g., List<Cat> <: List<Animal>)
 * - Contravariant (-): F<B> <: F<A>      (e.g., Consumer<Animal> <: Consumer<Cat>)
 * - Invariant (=):     no relationship    (e.g., MutableRef<Cat> ≠ MutableRef<Animal>)
 * - Bivariant (±):     both directions    (phantom type parameter)
 */

// Variance kinds
const COVARIANT = '+';       // Output position
const CONTRAVARIANT = '-';   // Input position
const INVARIANT = '=';       // Both positions (read+write)
const BIVARIANT = '±';       // Neither position (phantom)

// Types
class TBase { constructor(name) { this.tag = 'TBase'; this.name = name; } toString() { return this.name; } }
class TApp { constructor(ctor, args) { this.tag = 'TApp'; this.ctor = ctor; this.args = args; } toString() { return `${this.ctor}<${this.args.join(', ')}>`; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }

// Type constructors with variance annotations
class TypeCtor {
  constructor(name, params) {
    this.name = name;
    this.params = params; // [{name, variance}]
  }
  
  toString() {
    return `${this.name}<${this.params.map(p => `${p.variance}${p.name}`).join(', ')}>`;
  }
}

// Base subtyping (simple hierarchy)
const hierarchy = new Map([
  ['Cat', 'Animal'],
  ['Dog', 'Animal'],
  ['Animal', 'Object'],
  ['Int', 'Number'],
  ['Number', 'Object'],
  ['String', 'Object'],
  ['Nothing', 'Cat'], ['Nothing', 'Dog'], ['Nothing', 'Int'], ['Nothing', 'String'],
]);

function isBaseSubtype(a, b) {
  if (a === b) return true;
  if (a === 'Nothing') return true;
  const parent = hierarchy.get(a);
  if (!parent) return false;
  return parent === b || isBaseSubtype(parent, b);
}

/**
 * Check subtyping with variance
 */
function isSubtype(t1, t2, ctors = new Map()) {
  if (t1.tag === 'TBase' && t2.tag === 'TBase') return isBaseSubtype(t1.name, t2.name);
  
  if (t1.tag === 'TFun' && t2.tag === 'TFun') {
    // Functions: contravariant param, covariant return
    return isSubtype(t2.param, t1.param, ctors) && isSubtype(t1.ret, t2.ret, ctors);
  }
  
  if (t1.tag === 'TApp' && t2.tag === 'TApp' && t1.ctor === t2.ctor) {
    const ctor = ctors.get(t1.ctor);
    if (!ctor) return false;
    
    for (let i = 0; i < ctor.params.length; i++) {
      const variance = ctor.params[i].variance;
      const a = t1.args[i];
      const b = t2.args[i];
      
      switch (variance) {
        case COVARIANT:
          if (!isSubtype(a, b, ctors)) return false;
          break;
        case CONTRAVARIANT:
          if (!isSubtype(b, a, ctors)) return false;
          break;
        case INVARIANT:
          if (!isSubtype(a, b, ctors) || !isSubtype(b, a, ctors)) return false;
          break;
        case BIVARIANT:
          // Always OK
          break;
      }
    }
    return true;
  }
  
  return false;
}

/**
 * Infer variance of a type parameter from its usage positions
 */
function inferVariance(param, type) {
  const positions = collectPositions(param, type, true);
  const hasCovariant = positions.has('covariant');
  const hasContravariant = positions.has('contravariant');
  
  if (hasCovariant && hasContravariant) return INVARIANT;
  if (hasCovariant) return COVARIANT;
  if (hasContravariant) return CONTRAVARIANT;
  return BIVARIANT;
}

function collectPositions(param, type, positive, positions = new Set()) {
  switch (type.tag) {
    case 'TBase':
      if (type.name === param) {
        positions.add(positive ? 'covariant' : 'contravariant');
      }
      break;
    case 'TFun':
      collectPositions(param, type.param, !positive, positions);  // Flip for param
      collectPositions(param, type.ret, positive, positions);
      break;
    case 'TApp':
      for (const arg of type.args) {
        collectPositions(param, arg, positive, positions);
      }
      break;
  }
  return positions;
}

/**
 * Compose variances
 */
function composeVariance(outer, inner) {
  if (outer === BIVARIANT || inner === BIVARIANT) return BIVARIANT;
  if (outer === INVARIANT || inner === INVARIANT) return INVARIANT;
  if (outer === inner) return COVARIANT;
  return CONTRAVARIANT;
}

export {
  COVARIANT, CONTRAVARIANT, INVARIANT, BIVARIANT,
  TBase, TApp, TFun, TypeCtor,
  isSubtype, isBaseSubtype, inferVariance, composeVariance
};
