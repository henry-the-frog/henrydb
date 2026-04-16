/**
 * Type Families: Type-level functions (GHC-style)
 */
class TypeFamily {
  constructor(name, equations) { this.name = name; this.equations = equations; }
  apply(...args) {
    for (const eq of this.equations) {
      const match = eq.match(args);
      if (match) return match;
    }
    throw new Error(`No match for ${this.name}(${args.join(', ')})`);
  }
}

class Equation { constructor(patterns, result) { this.patterns = patterns; this.result = result; }
  match(args) {
    if (args.length !== this.patterns.length) return null;
    const bindings = new Map();
    for (let i = 0; i < args.length; i++) {
      const p = this.patterns[i];
      if (typeof p === 'string' && p.startsWith('$')) { bindings.set(p, args[i]); continue; }
      if (p !== args[i]) return null;
    }
    let result = this.result;
    if (typeof result === 'function') return result(bindings);
    return result;
  }
}

// Examples
const Add1 = new TypeFamily('Add1', [
  new Equation(['Z'], 'S(Z)'),
  new Equation(['$n'], b => `S(${b.get('$n')})`),
]);

const Append = new TypeFamily('Append', [
  new Equation(['Nil', '$ys'], b => b.get('$ys')),
  new Equation(['$xs', '$ys'], b => `Cons(_, ${b.get('$ys')})`),
]);

const If = new TypeFamily('If', [
  new Equation(['True', '$t', '$f'], b => b.get('$t')),
  new Equation(['False', '$t', '$f'], b => b.get('$f')),
]);

export { TypeFamily, Equation, Add1, Append, If };
