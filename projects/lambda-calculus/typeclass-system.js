/**
 * Type Classes: Resolution, coherence, and instance search
 * 
 * A more detailed implementation of Haskell-style type classes:
 * - Class declarations with superclasses
 * - Instance declarations with constraints
 * - Instance resolution with backtracking
 * - Coherence checking (no overlapping instances)
 */

class TypeClass {
  constructor(name, params, superclasses = [], methods = []) {
    this.name = name;
    this.params = params;
    this.superclasses = superclasses;
    this.methods = methods;
  }
}

class Instance {
  constructor(className, typeArgs, constraints = [], impl = {}) {
    this.className = className;
    this.typeArgs = typeArgs;
    this.constraints = constraints; // [{class: 'Eq', type: 'a'}]
    this.impl = impl;
  }
}

class ClassEnv {
  constructor() { this.classes = new Map(); this.instances = []; }
  
  addClass(cls) { this.classes.set(cls.name, cls); }
  
  addInstance(inst) {
    // Check for overlapping instances
    const overlapping = this.instances.find(i =>
      i.className === inst.className && typesOverlap(i.typeArgs, inst.typeArgs)
    );
    if (overlapping) throw new Error(`Overlapping instances for ${inst.className}`);
    this.instances.push(inst);
  }
  
  resolve(className, typeArg) {
    const matching = this.instances.filter(i =>
      i.className === className && typeMatches(i.typeArgs[0], typeArg)
    );
    if (matching.length === 0) return null;
    if (matching.length > 1) throw new Error(`Ambiguous: multiple instances for ${className} ${typeArg}`);
    
    const inst = matching[0];
    // Check constraints
    for (const c of inst.constraints) {
      const resolved = this.resolve(c.class, typeArg);
      if (!resolved) return null;
    }
    return inst;
  }
  
  hasSuperclass(className, superName) {
    const cls = this.classes.get(className);
    if (!cls) return false;
    if (cls.superclasses.includes(superName)) return true;
    return cls.superclasses.some(s => this.hasSuperclass(s, superName));
  }
}

function typeMatches(pattern, concrete) {
  if (pattern === concrete) return true;
  if (pattern.match && pattern.match(/^[a-z]/)) return true; // Type variable matches anything
  return false;
}

function typesOverlap(args1, args2) {
  return args1.every((a, i) => {
    if (a.match && a.match(/^[a-z]/)) return true;
    if (args2[i].match && args2[i].match(/^[a-z]/)) return true;
    return a === args2[i];
  });
}

export { TypeClass, Instance, ClassEnv };
