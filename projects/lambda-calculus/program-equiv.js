/**
 * Program Equivalence: Check if two programs are observationally equal
 * 
 * Two programs are equivalent if they produce the same observable behavior
 * for all possible inputs.
 */

class Var { constructor(name) { this.tag = 'Var'; this.name = name; } toString() { return this.name; } }
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } toString() { return `${this.n}`; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}.${this.body})`; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class Add { constructor(l, r) { this.tag = 'Add'; this.left = l; this.right = r; } toString() { return `(${this.left}+${this.right})`; } }

function subst(e, n, r) {
  switch (e.tag) {
    case 'Var': return e.name === n ? r : e;
    case 'Num': return e;
    case 'Lam': return e.var === n ? e : new Lam(e.var, subst(e.body, n, r));
    case 'App': return new App(subst(e.fn, n, r), subst(e.arg, n, r));
    case 'Add': return new Add(subst(e.left, n, r), subst(e.right, n, r));
  }
}

function eval_(e, fuel = 100) {
  if (fuel <= 0) return null;
  switch (e.tag) {
    case 'Num': return e.n;
    case 'Var': return null;
    case 'Add': { const l = eval_(e.left, fuel - 1), r = eval_(e.right, fuel - 1); return l !== null && r !== null ? l + r : null; }
    case 'App': {
      if (e.fn.tag === 'Lam') return eval_(subst(e.fn.body, e.fn.var, e.arg), fuel - 1);
      return null;
    }
    case 'Lam': return e; // Return as-is
  }
}

// Observational equivalence: test with many inputs
function obsEqual(e1, e2, testInputs = [0, 1, -1, 42, 100]) {
  for (const input of testInputs) {
    const r1 = eval_(new App(e1, new Num(input)));
    const r2 = eval_(new App(e2, new Num(input)));
    if (r1 === null && r2 === null) continue;
    if (r1 === null || r2 === null) return { equal: false, witness: input, r1, r2 };
    if (typeof r1 === 'number' && typeof r2 === 'number' && r1 !== r2) return { equal: false, witness: input, r1, r2 };
  }
  return { equal: true };
}

// Structural equivalence (modulo alpha)
function structEqual(e1, e2, env = new Map()) {
  if (e1.tag !== e2.tag) return false;
  switch (e1.tag) {
    case 'Num': return e1.n === e2.n;
    case 'Var': {
      const m1 = env.get(e1.name + ':1'), m2 = env.get(e2.name + ':2');
      if (m1 !== undefined || m2 !== undefined) return m1 === m2;
      return e1.name === e2.name;
    }
    case 'Lam': {
      const depth = env.size;
      const newEnv = new Map([...env, [e1.var + ':1', depth], [e2.var + ':2', depth]]);
      return structEqual(e1.body, e2.body, newEnv);
    }
    case 'App': return structEqual(e1.fn, e2.fn, env) && structEqual(e1.arg, e2.arg, env);
    case 'Add': return structEqual(e1.left, e2.left, env) && structEqual(e1.right, e2.right, env);
  }
}

// Equivalence classes
function groupEquivalent(exprs, compareFn = obsEqual) {
  const groups = [];
  for (const e of exprs) {
    let added = false;
    for (const g of groups) {
      if (compareFn(g[0], e).equal) { g.push(e); added = true; break; }
    }
    if (!added) groups.push([e]);
  }
  return groups;
}

export { Var, Num, Lam, App, Add, eval_, obsEqual, structEqual, groupEquivalent };
