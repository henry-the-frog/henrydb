/**
 * Substitution Calculus: Explicit substitution as first-class
 */
class Var { constructor(i) { this.tag='Var'; this.idx=i; } }
class Lam { constructor(b) { this.tag='Lam'; this.body=b; } }
class App { constructor(f,a) { this.tag='App'; this.fn=f; this.arg=a; } }
class Clos { constructor(t,s) { this.tag='Clos'; this.term=t; this.subst=s; } }
class Id { constructor() { this.tag='Id'; } }
class Shift { constructor() { this.tag='Shift'; } }
class Cons { constructor(t,s) { this.tag='Cons'; this.term=t; this.subst=s; } }
class Comp { constructor(s1,s2) { this.tag='Comp'; this.s1=s1; this.s2=s2; } }

function applySubst(s, t) {
  if (t.tag === 'Var') return lookup(s, t.idx);
  if (t.tag === 'App') return new App(applySubst(s, t.fn), applySubst(s, t.arg));
  if (t.tag === 'Lam') return new Lam(applySubst(new Cons(new Var(0), new Comp(s, new Shift())), t.body));
  if (t.tag === 'Clos') return applySubst(composeS(t.subst, s), t.term);
  return t;
}

function lookup(s, i) {
  if (s.tag === 'Id') return new Var(i);
  if (s.tag === 'Shift') return new Var(i + 1);
  if (s.tag === 'Cons') return i === 0 ? s.term : lookup(s.subst, i - 1);
  if (s.tag === 'Comp') return applySubst(s.s2, lookup(s.s1, i));
  return new Var(i);
}

function composeS(s1, s2) {
  if (s1.tag === 'Id') return s2;
  if (s2.tag === 'Id') return s1;
  return new Comp(s1, s2);
}

function beta(body, arg) { return applySubst(new Cons(arg, new Id()), body); }

export { Var, Lam, App, Clos, Id, Shift, Cons, Comp, applySubst, lookup, composeS, beta };
