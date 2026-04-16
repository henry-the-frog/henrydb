/**
 * Name Supply: Monadic fresh name generation
 * 
 * Type inference, compiler passes, and code generation all need fresh names.
 * A name supply makes this composable and deterministic.
 */

class NameSupply {
  constructor(prefix = 'x', start = 0) {
    this.prefix = prefix;
    this.counter = start;
    this.used = new Set();
  }

  fresh(hint = null) {
    const base = hint || this.prefix;
    let name;
    do { name = `${base}${this.counter++}`; } while (this.used.has(name));
    this.used.add(name);
    return name;
  }

  freshN(n, hint = null) {
    return Array.from({ length: n }, () => this.fresh(hint));
  }

  // Mark a name as used (avoid conflicts with user names)
  reserve(name) { this.used.add(name); }
  reserveAll(names) { names.forEach(n => this.used.add(n)); }
  
  isUsed(name) { return this.used.has(name); }
  
  // Create child supply with different prefix (for scoping)
  child(prefix) { return new NameSupply(prefix, 0); }
  
  // Snapshot for backtracking
  snapshot() { return { counter: this.counter, used: new Set(this.used) }; }
  restore(snap) { this.counter = snap.counter; this.used = new Set(snap.used); }
}

// Functional name supply (returns [name, newSupply] pair)
function freshFrom(supply) {
  const name = supply.fresh();
  return [name, supply];
}

// Name avoidance: find a name NOT in the given set
function avoid(name, forbidden) {
  if (!forbidden.has(name)) return name;
  let i = 0;
  while (forbidden.has(`${name}${i}`)) i++;
  return `${name}${i}`;
}

// Rename to avoid conflicts
function alpha(expr, forbidden, supply) {
  switch (expr.tag) {
    case 'Var': return expr;
    case 'Lam': {
      const newName = avoid(expr.var, forbidden);
      const newForbidden = new Set([...forbidden, newName]);
      const body = expr.var === newName ? expr.body : substSimple(expr.body, expr.var, { tag: 'Var', name: newName });
      return { tag: 'Lam', var: newName, body: alpha(body, newForbidden, supply) };
    }
    case 'App': return { tag: 'App', fn: alpha(expr.fn, forbidden, supply), arg: alpha(expr.arg, forbidden, supply) };
    default: return expr;
  }
}

function substSimple(expr, name, repl) {
  if (expr.tag === 'Var') return expr.name === name ? repl : expr;
  if (expr.tag === 'Lam') return expr.var === name ? expr : { tag: 'Lam', var: expr.var, body: substSimple(expr.body, name, repl) };
  if (expr.tag === 'App') return { tag: 'App', fn: substSimple(expr.fn, name, repl), arg: substSimple(expr.arg, name, repl) };
  return expr;
}

export { NameSupply, freshFrom, avoid, alpha };
