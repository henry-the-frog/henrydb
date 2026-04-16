/**
 * First-Class Modules
 * 
 * Modules as values that can be passed to functions, stored in data structures.
 * Inspired by ML modules / 1ML / Modular Type Classes.
 * 
 * A module packages: type definitions + value definitions
 * A signature describes: type requirements + value types
 */

class Signature {
  constructor(name, types, values) {
    this.name = name;
    this.types = types;   // Map<name, kind> (abstract types)
    this.values = values; // Map<name, type>
  }
  
  satisfiedBy(mod) {
    for (const [name, _] of this.types) {
      if (!mod.types.has(name)) return { ok: false, error: `Missing type: ${name}` };
    }
    for (const [name, _] of this.values) {
      if (!mod.values.has(name)) return { ok: false, error: `Missing value: ${name}` };
    }
    return { ok: true };
  }
}

class Module {
  constructor(name, types = new Map(), values = new Map()) {
    this.name = name;
    this.types = types;   // Map<name, definition>
    this.values = values; // Map<name, implementation>
  }

  getType(name) { return this.types.get(name); }
  getValue(name) { return this.values.get(name); }
  
  extend(name, types, values) {
    return new Module(name,
      new Map([...this.types, ...types]),
      new Map([...this.values, ...values])
    );
  }
}

// ============================================================
// Functor: module → module
// ============================================================

class Functor {
  constructor(name, paramSig, bodyFn) {
    this.name = name;
    this.paramSig = paramSig;
    this.bodyFn = bodyFn;
  }

  apply(argModule) {
    const check = this.paramSig.satisfiedBy(argModule);
    if (!check.ok) throw new Error(`Functor ${this.name}: ${check.error}`);
    return this.bodyFn(argModule);
  }
}

// ============================================================
// Example: Comparable → SortedSet
// ============================================================

const ComparableSig = new Signature('Comparable',
  new Map([['T', '*']]),
  new Map([['compare', '(T, T) → Int'], ['eq', '(T, T) → Bool']])
);

const IntComparable = new Module('IntComparable',
  new Map([['T', 'Int']]),
  new Map([
    ['compare', (a, b) => a - b],
    ['eq', (a, b) => a === b]
  ])
);

const StrComparable = new Module('StrComparable',
  new Map([['T', 'String']]),
  new Map([
    ['compare', (a, b) => a < b ? -1 : a > b ? 1 : 0],
    ['eq', (a, b) => a === b]
  ])
);

const MakeSortedSet = new Functor('MakeSortedSet', ComparableSig, (C) => {
  const compare = C.getValue('compare');
  return new Module(`SortedSet(${C.name})`,
    new Map([['T', 'SortedSet']]),
    new Map([
      ['empty', () => []],
      ['insert', (x, set) => {
        if (set.some(e => C.getValue('eq')(e, x))) return set;
        return [...set, x].sort(compare);
      }],
      ['contains', (x, set) => set.some(e => C.getValue('eq')(e, x))],
      ['toList', set => [...set]],
      ['size', set => set.length],
    ])
  );
});

export { Signature, Module, Functor, ComparableSig, IntComparable, StrComparable, MakeSortedSet };
