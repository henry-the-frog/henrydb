/**
 * Abstract Binding Trees (ABTs): Variable binding as a data structure
 * 
 * An ABT is like an AST but with first-class support for variable binding.
 * Variables and binding are part of the tree structure, not ad-hoc.
 */

class AVar { constructor(name) { this.tag = 'AVar'; this.name = name; } toString() { return this.name; } }
class ABind { constructor(v, body) { this.tag = 'ABind'; this.var = v; this.body = body; } toString() { return `${this.var}.${this.body}`; } }
class AOp { constructor(name, args) { this.tag = 'AOp'; this.name = name; this.args = args; } toString() { return `${this.name}(${this.args.join(', ')})`; } }

// Operations with binding structure
function lam(v, body) { return new AOp('lam', [new ABind(v, body)]); }
function app(fn, arg) { return new AOp('app', [fn, arg]); }
function let_(v, init, body) { return new AOp('let', [init, new ABind(v, body)]); }
function num(n) { return new AOp('num', [new AVar(String(n))]); }

// Free variables
function freeVars(abt) {
  switch (abt.tag) {
    case 'AVar': return new Set([abt.name]);
    case 'ABind': { const fv = freeVars(abt.body); fv.delete(abt.var); return fv; }
    case 'AOp': return new Set(abt.args.flatMap(a => [...freeVars(a)]));
  }
}

// Substitute
function subst(abt, name, repl) {
  switch (abt.tag) {
    case 'AVar': return abt.name === name ? repl : abt;
    case 'ABind': return abt.var === name ? abt : new ABind(abt.var, subst(abt.body, name, repl));
    case 'AOp': return new AOp(abt.name, abt.args.map(a => subst(a, name, repl)));
  }
}

// Alpha-equivalent
function alphaEq(a, b, env = new Map(), depth = 0) {
  if (a.tag !== b.tag) return false;
  switch (a.tag) {
    case 'AVar': {
      const da = env.has(a.name) ? env.get(a.name) : a.name;
      const db = env.has(b.name) ? env.get(b.name) : b.name;
      return da === db;
    }
    case 'ABind':
      return alphaEq(a.body, b.body, new Map([...env, [a.var, `L${depth}`], [b.var, `L${depth}`]]), depth + 1);
    case 'AOp':
      return a.name === b.name && a.args.length === b.args.length && a.args.every((arg, i) => alphaEq(arg, b.args[i], env, depth));
  }
}

// Validate well-scoped
function wellScoped(abt, scope = new Set()) {
  switch (abt.tag) {
    case 'AVar': return scope.has(abt.name);
    case 'ABind': return wellScoped(abt.body, new Set([...scope, abt.var]));
    case 'AOp': return abt.args.every(a => wellScoped(a, scope));
  }
}

export { AVar, ABind, AOp, lam, app, let_, num, freeVars, subst, alphaEq, wellScoped };
