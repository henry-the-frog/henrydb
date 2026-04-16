/**
 * Lambda Cube (Barendregt 1991)
 * 
 * 8 type systems along 3 axes:
 * 1. Terms depending on terms: λ→ (STLC)
 * 2. Terms depending on types: System F (polymorphism)
 * 3. Types depending on types: Fω (type operators)
 * 4. Types depending on terms: λP (dependent types)
 * 
 * Combinations give 8 systems:
 * - λ→: STLC (nothing)
 * - λ2: System F (polymorphism only)
 * - λω: type operators only
 * - λP: dependent types only
 * - λ2ω: System Fω
 * - λ2P: no standard name
 * - λωP: no standard name
 * - λ2ωP: Calculus of Constructions (all three)
 */

// Features
const POLYMORPHISM = 'polymorphism';   // Terms depend on types (Λα.t)
const TYPE_OPERATORS = 'type_ops';     // Types depend on types (λα:*.τ)
const DEPENDENT_TYPES = 'dependent';   // Types depend on terms (Π(x:A).B(x))

class System {
  constructor(name, features, description) {
    this.name = name;
    this.features = new Set(features);
    this.description = description;
  }
  
  has(feature) { return this.features.has(feature); }
  
  isSubsystemOf(other) {
    return [...this.features].every(f => other.features.has(f));
  }
  
  toString() { return `${this.name}: ${this.description}`; }
}

// The 8 systems of the lambda cube
const STLC = new System('λ→', [], 'Simply Typed Lambda Calculus');
const SystemF = new System('λ2', [POLYMORPHISM], 'System F (polymorphism)');
const Fomega = new System('λω', [TYPE_OPERATORS], 'Type operators');
const LambdaP = new System('λP', [DEPENDENT_TYPES], 'Dependent types (LF)');
const SystemFomega = new System('λ2ω', [POLYMORPHISM, TYPE_OPERATORS], 'System Fω');
const Lambda2P = new System('λ2P', [POLYMORPHISM, DEPENDENT_TYPES], 'Polymorphism + dependent types');
const LambdaOmegaP = new System('λωP', [TYPE_OPERATORS, DEPENDENT_TYPES], 'Type operators + dependent types');
const CoC = new System('λ2ωP', [POLYMORPHISM, TYPE_OPERATORS, DEPENDENT_TYPES], 'Calculus of Constructions');

const lambdaCube = [STLC, SystemF, Fomega, LambdaP, SystemFomega, Lambda2P, LambdaOmegaP, CoC];

// Expressiveness examples
function whatCanExpress(system) {
  const abilities = [];
  if (system.has(POLYMORPHISM)) abilities.push('∀α. α → α (identity for all types)');
  if (system.has(TYPE_OPERATORS)) abilities.push('List : * → * (parameterized types)');
  if (system.has(DEPENDENT_TYPES)) abilities.push('Vec : Nat → * → * (length-indexed)');
  if (!system.has(POLYMORPHISM) && !system.has(TYPE_OPERATORS) && !system.has(DEPENDENT_TYPES)) {
    abilities.push('Int → Bool (monomorphic functions only)');
  }
  return abilities;
}

// Check subsystem relations
function subsystemRelations() {
  const relations = [];
  for (const a of lambdaCube) {
    for (const b of lambdaCube) {
      if (a !== b && a.isSubsystemOf(b)) {
        relations.push(`${a.name} ⊆ ${b.name}`);
      }
    }
  }
  return relations;
}

// Find systems with given features
function findSystem(features) {
  return lambdaCube.find(s =>
    features.length === s.features.size &&
    features.every(f => s.has(f))
  );
}

export {
  POLYMORPHISM, TYPE_OPERATORS, DEPENDENT_TYPES,
  System, STLC, SystemF, Fomega, LambdaP, SystemFomega, Lambda2P, LambdaOmegaP, CoC,
  lambdaCube, whatCanExpress, subsystemRelations, findSystem
};
