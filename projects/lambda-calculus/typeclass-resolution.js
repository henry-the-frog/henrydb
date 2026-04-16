/**
 * Type Class Resolution
 * 
 * Resolve type class constraints to concrete instances.
 * Handles: instance search, superclasses, derived instances.
 * 
 * Example: Show [Int] requires Show Int (which exists), so it can derive Show [Int].
 */

class TypeClass {
  constructor(name, superclasses = []) {
    this.name = name;
    this.superclasses = superclasses;
    this.methods = new Map();
  }
  addMethod(name, type) { this.methods.set(name, type); return this; }
}

class Instance {
  constructor(className, typeName, methods, constraints = []) {
    this.className = className;
    this.typeName = typeName;
    this.methods = methods;       // Map<name, implementation>
    this.constraints = constraints; // [{className, typeName}] (required constraints)
  }
  key() { return `${this.className}:${this.typeName}`; }
}

class Resolver {
  constructor() {
    this.classes = new Map();     // name → TypeClass
    this.instances = new Map();   // "Class:Type" → Instance
  }

  addClass(tc) { this.classes.set(tc.name, tc); return this; }
  
  addInstance(inst) {
    this.instances.set(inst.key(), inst);
    return this;
  }

  /**
   * Resolve a constraint: find an instance for Class(Type)
   */
  resolve(className, typeName, depth = 0) {
    if (depth > 20) return { ok: false, error: 'Resolution depth exceeded (possible loop)' };
    
    const key = `${className}:${typeName}`;
    const inst = this.instances.get(key);
    
    if (!inst) {
      return { ok: false, error: `No instance: ${className} ${typeName}` };
    }
    
    // Check all constraints
    for (const constraint of inst.constraints) {
      const sub = this.resolve(constraint.className, constraint.typeName, depth + 1);
      if (!sub.ok) return sub;
    }
    
    // Check superclass constraints
    const tc = this.classes.get(className);
    if (tc) {
      for (const superclass of tc.superclasses) {
        const sub = this.resolve(superclass, typeName, depth + 1);
        if (!sub.ok) return { ok: false, error: `Superclass ${superclass} not satisfied for ${typeName}` };
      }
    }
    
    return { ok: true, instance: inst };
  }

  /**
   * Dispatch a method call
   */
  dispatch(className, typeName, methodName) {
    const result = this.resolve(className, typeName);
    if (!result.ok) throw new Error(result.error);
    
    const method = result.instance.methods.get(methodName);
    if (!method) throw new Error(`No method ${methodName} in ${className} ${typeName}`);
    return method;
  }

  /**
   * List all instances of a class
   */
  instancesOf(className) {
    return [...this.instances.values()].filter(i => i.className === className);
  }
}

// ============================================================
// Standard library
// ============================================================

function createStdLib() {
  const resolver = new Resolver();
  
  // Classes
  const Eq = new TypeClass('Eq');
  Eq.addMethod('eq', '(a, a) → Bool');
  
  const Ord = new TypeClass('Ord', ['Eq']);
  Ord.addMethod('compare', '(a, a) → Ordering');
  
  const Show = new TypeClass('Show');
  Show.addMethod('show', 'a → String');
  
  const Functor = new TypeClass('Functor');
  Functor.addMethod('fmap', '(a → b) → f a → f b');
  
  resolver.addClass(Eq).addClass(Ord).addClass(Show).addClass(Functor);
  
  // Instances
  resolver.addInstance(new Instance('Eq', 'Int', new Map([['eq', (a, b) => a === b]])));
  resolver.addInstance(new Instance('Eq', 'String', new Map([['eq', (a, b) => a === b]])));
  resolver.addInstance(new Instance('Eq', 'Bool', new Map([['eq', (a, b) => a === b]])));
  resolver.addInstance(new Instance('Ord', 'Int', new Map([['compare', (a, b) => a < b ? -1 : a > b ? 1 : 0]])));
  resolver.addInstance(new Instance('Show', 'Int', new Map([['show', x => String(x)]])));
  resolver.addInstance(new Instance('Show', 'String', new Map([['show', x => `"${x}"`]])));
  resolver.addInstance(new Instance('Show', 'Bool', new Map([['show', x => String(x)]])));
  
  // Derived instance: Eq [Int] requires Eq Int
  resolver.addInstance(new Instance('Eq', '[Int]',
    new Map([['eq', (a, b) => JSON.stringify(a) === JSON.stringify(b)]]),
    [{ className: 'Eq', typeName: 'Int' }]));
  
  return resolver;
}

export { TypeClass, Instance, Resolver, createStdLib };
