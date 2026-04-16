/**
 * Typeclass Dictionary Passing
 * 
 * Transforms typeclass constraints into explicit dictionary arguments.
 * This is how Haskell compiles typeclasses (GHC dictionary passing).
 * 
 * Before: show :: Show a => a -> String
 * After:  show :: ShowDict a -> a -> String
 * 
 * Before: show 42
 * After:  show showIntDict 42
 */

class TypeClass {
  constructor(name, methods) {
    this.name = name;
    this.methods = methods; // [{name, type}]
  }
}

class Instance {
  constructor(className, typeName, implementations) {
    this.className = className;
    this.typeName = typeName;
    this.implementations = implementations; // Map<methodName, impl>
  }
}

class Dictionary {
  constructor(className, typeName, methods) {
    this.tag = 'Dict';
    this.className = className;
    this.typeName = typeName;
    this.methods = methods; // Map<methodName, function>
  }
  
  call(methodName, ...args) {
    const method = this.methods.get(methodName);
    if (!method) throw new Error(`No method ${methodName} in ${this.className} ${this.typeName}`);
    return method(...args);
  }
}

// ============================================================
// Typeclass Registry
// ============================================================

class TypeclassRegistry {
  constructor() {
    this.classes = new Map();    // className → TypeClass
    this.instances = new Map();  // "className:typeName" → Dictionary
  }

  defineClass(name, methods) {
    this.classes.set(name, new TypeClass(name, methods));
  }

  addInstance(className, typeName, implementations) {
    const key = `${className}:${typeName}`;
    this.instances.set(key, new Dictionary(className, typeName, new Map(Object.entries(implementations))));
  }

  getDictionary(className, typeName) {
    return this.instances.get(`${className}:${typeName}`);
  }

  /**
   * Transform a constrained call into dictionary-passing style.
   * 
   * constrained('Show', 'Int', 'show', [42])
   *   → lookup ShowDict for Int, call show(42)
   */
  dispatch(className, typeName, methodName, args) {
    const dict = this.getDictionary(className, typeName);
    if (!dict) throw new Error(`No instance ${className} ${typeName}`);
    return dict.call(methodName, ...args);
  }
}

// ============================================================
// Standard typeclasses
// ============================================================

function createStandardRegistry() {
  const reg = new TypeclassRegistry();
  
  // Show
  reg.defineClass('Show', [{ name: 'show', type: 'a -> String' }]);
  reg.addInstance('Show', 'Int', { show: x => `${x}` });
  reg.addInstance('Show', 'Bool', { show: x => x ? 'True' : 'False' });
  reg.addInstance('Show', 'String', { show: x => `"${x}"` });
  
  // Eq
  reg.defineClass('Eq', [{ name: 'eq', type: 'a -> a -> Bool' }, { name: 'neq', type: 'a -> a -> Bool' }]);
  reg.addInstance('Eq', 'Int', { eq: (x, y) => x === y, neq: (x, y) => x !== y });
  reg.addInstance('Eq', 'Bool', { eq: (x, y) => x === y, neq: (x, y) => x !== y });
  reg.addInstance('Eq', 'String', { eq: (x, y) => x === y, neq: (x, y) => x !== y });
  
  // Ord (extends Eq)
  reg.defineClass('Ord', [{ name: 'compare', type: 'a -> a -> Ordering' }, { name: 'lt', type: 'a -> a -> Bool' }]);
  reg.addInstance('Ord', 'Int', { compare: (x, y) => x < y ? -1 : x > y ? 1 : 0, lt: (x, y) => x < y });
  reg.addInstance('Ord', 'String', { compare: (x, y) => x < y ? -1 : x > y ? 1 : 0, lt: (x, y) => x < y });
  
  // Num
  reg.defineClass('Num', [{ name: 'add', type: 'a -> a -> a' }, { name: 'mul', type: 'a -> a -> a' }, { name: 'fromInteger', type: 'Int -> a' }]);
  reg.addInstance('Num', 'Int', { add: (x, y) => x + y, mul: (x, y) => x * y, fromInteger: x => x });
  
  // Functor
  reg.defineClass('Functor', [{ name: 'fmap', type: '(a -> b) -> f a -> f b' }]);
  reg.addInstance('Functor', 'Array', { fmap: (f, xs) => xs.map(f) });
  reg.addInstance('Functor', 'Maybe', { fmap: (f, m) => m === null ? null : f(m) });
  
  return reg;
}

export { TypeClass, Instance, Dictionary, TypeclassRegistry, createStandardRegistry };
