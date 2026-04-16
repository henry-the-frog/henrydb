/**
 * Constraint Solving: Generate and solve type constraints
 * 
 * Two-phase type inference:
 * 1. Generate: walk AST, emit constraints (τ₁ = τ₂)
 * 2. Solve: unify all constraints simultaneously
 */

class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }
class TCon { constructor(name) { this.tag = 'TCon'; this.name = name; } toString() { return this.name; } }

class Constraint {
  constructor(left, right, reason) { this.left = left; this.right = right; this.reason = reason; }
  toString() { return `${this.left} = ${this.right} [${this.reason}]`; }
}

class Substitution {
  constructor(map = new Map()) { this.map = map; }
  
  apply(type) {
    switch (type.tag) {
      case 'TVar': return this.map.has(type.name) ? this.apply(this.map.get(type.name)) : type;
      case 'TFun': return new TFun(this.apply(type.param), this.apply(type.ret));
      case 'TCon': return type;
    }
  }
  
  compose(other) {
    const newMap = new Map();
    for (const [k, v] of other.map) newMap.set(k, this.apply(v));
    for (const [k, v] of this.map) if (!newMap.has(k)) newMap.set(k, v);
    return new Substitution(newMap);
  }
  
  extend(name, type) { return new Substitution(new Map([...this.map, [name, type]])); }
}

function occurs(name, type) {
  switch (type.tag) {
    case 'TVar': return type.name === name;
    case 'TFun': return occurs(name, type.param) || occurs(name, type.ret);
    case 'TCon': return false;
  }
}

function solveConstraints(constraints) {
  let subst = new Substitution();
  const remaining = [...constraints];
  
  while (remaining.length > 0) {
    const c = remaining.shift();
    const left = subst.apply(c.left);
    const right = subst.apply(c.right);
    
    if (left.toString() === right.toString()) continue;
    
    if (left.tag === 'TVar') {
      if (occurs(left.name, right)) return { ok: false, error: `Infinite type: ${left.name} in ${right}` };
      subst = subst.extend(left.name, right);
      continue;
    }
    if (right.tag === 'TVar') {
      if (occurs(right.name, left)) return { ok: false, error: `Infinite type: ${right.name} in ${left}` };
      subst = subst.extend(right.name, left);
      continue;
    }
    if (left.tag === 'TFun' && right.tag === 'TFun') {
      remaining.push(new Constraint(left.param, right.param, `${c.reason}/param`));
      remaining.push(new Constraint(left.ret, right.ret, `${c.reason}/ret`));
      continue;
    }
    if (left.tag === 'TCon' && right.tag === 'TCon' && left.name === right.name) continue;
    
    return { ok: false, error: `Cannot unify ${left} with ${right}`, constraint: c };
  }
  
  return { ok: true, subst };
}

export { TVar, TFun, TCon, Constraint, Substitution, solveConstraints };
