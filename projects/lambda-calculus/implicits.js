/**
 * Coherent Implicit Parameters: Scala-style implicits/Haskell type classes
 * 
 * Values resolved automatically from scope based on type.
 * Coherence: at most one value of each type in scope.
 */

class ImplicitScope {
  constructor(parent = null) { this.bindings = new Map(); this.parent = parent; }
  
  provide(type, value) {
    if (this.bindings.has(type)) throw new Error(`Incoherent: duplicate implicit for ${type}`);
    this.bindings.set(type, value);
    return this;
  }
  
  resolve(type) {
    if (this.bindings.has(type)) return this.bindings.get(type);
    if (this.parent) return this.parent.resolve(type);
    throw new Error(`No implicit for ${type}`);
  }
  
  has(type) {
    if (this.bindings.has(type)) return true;
    return this.parent ? this.parent.has(type) : false;
  }
  
  child() { return new ImplicitScope(this); }
}

// Implicit function: automatically resolves args from scope
function withImplicits(scope, fn) {
  return (...explicitArgs) => fn(scope, ...explicitArgs);
}

// Show typeclass via implicits
function show(scope, value) {
  const shower = scope.resolve(`Show<${typeof value}>`);
  return shower(value);
}

function eq(scope, a, b) {
  const eqFn = scope.resolve(`Eq<${typeof a}>`);
  return eqFn(a, b);
}

// Standard instances
function standardScope() {
  const scope = new ImplicitScope();
  scope.provide('Show<number>', n => `${n}`);
  scope.provide('Show<string>', s => `"${s}"`);
  scope.provide('Show<boolean>', b => `${b}`);
  scope.provide('Eq<number>', (a, b) => a === b);
  scope.provide('Eq<string>', (a, b) => a === b);
  scope.provide('Eq<boolean>', (a, b) => a === b);
  scope.provide('Ord<number>', (a, b) => a - b);
  return scope;
}

export { ImplicitScope, withImplicits, show, eq, standardScope };
