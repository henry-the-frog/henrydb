/**
 * Substitution Lemma: Key metatheoretic property
 */
class Var { constructor(n) { this.tag='Var'; this.name=n; } eq(o) { return o.tag==='Var'&&o.name===this.name; } }
class Lam { constructor(v,b) { this.tag='Lam'; this.var=v; this.body=b; } eq(o) { return o.tag==='Lam'&&this.body.eq(o.body); } }
class App { constructor(f,a) { this.tag='App'; this.fn=f; this.arg=a; } eq(o) { return o.tag==='App'&&this.fn.eq(o.fn)&&this.arg.eq(o.arg); } }

function subst(e, x, s) {
  switch(e.tag) {
    case 'Var': return e.name === x ? s : e;
    case 'Lam': return e.var === x ? e : new Lam(e.var, subst(e.body, x, s));
    case 'App': return new App(subst(e.fn, x, s), subst(e.arg, x, s));
  }
}

function fv(e) {
  switch(e.tag) {
    case 'Var': return new Set([e.name]);
    case 'Lam': { const s = fv(e.body); s.delete(e.var); return s; }
    case 'App': return new Set([...fv(e.fn), ...fv(e.arg)]);
  }
}

// Substitution lemma: if x ∉ FV(s) or y ∉ FV(r), then
// [r/x]([s/y]e) = [[r/x]s/y]([r/x]e)
function checkSubstLemma(e, x, r, y, s) {
  if (x === y) return true;
  const lhs = subst(subst(e, y, s), x, r);
  const rhs = subst(subst(e, x, r), y, subst(s, x, r));
  return exprEq(lhs, rhs);
}

function exprEq(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'Var') return a.name === b.name;
  if (a.tag === 'Lam') return a.var === b.var && exprEq(a.body, b.body);
  if (a.tag === 'App') return exprEq(a.fn, b.fn) && exprEq(a.arg, b.arg);
  return false;
}

export { Var, Lam, App, subst, fv, checkSubstLemma, exprEq };
