/**
 * Module #188: Church-Rosser Theorem — Confluence of beta-reduction
 */
class Var { constructor(n) { this.tag='Var'; this.name=n; } }
class Lam { constructor(v,b) { this.tag='Lam'; this.var=v; this.body=b; } }
class App { constructor(f,a) { this.tag='App'; this.fn=f; this.arg=a; } }

function subst(e, x, s) {
  switch(e.tag) { case 'Var': return e.name===x?s:e; case 'Lam': return e.var===x?e:new Lam(e.var,subst(e.body,x,s)); case 'App': return new App(subst(e.fn,x,s),subst(e.arg,x,s)); }
}

function eq(a,b) { if(a.tag!==b.tag)return false; if(a.tag==='Var')return a.name===b.name; if(a.tag==='Lam')return a.var===b.var&&eq(a.body,b.body); return eq(a.fn,b.fn)&&eq(a.arg,b.arg); }

function step(e) {
  if(e.tag==='App'&&e.fn.tag==='Lam') return subst(e.fn.body,e.fn.var,e.arg);
  if(e.tag==='App') { const fn=step(e.fn); if(!eq(fn,e.fn)) return new App(fn,e.arg); return new App(e.fn,step(e.arg)); }
  if(e.tag==='Lam') return new Lam(e.var,step(e.body));
  return e;
}

function normalize(e, max=100) { for(let i=0;i<max;i++){const n=step(e);if(eq(n,e))return e;e=n;} return e; }

function checkConfluence(e1, e2) {
  const n1 = normalize(e1), n2 = normalize(e2);
  return eq(n1, n2);
}

function isNormal(e) { return eq(e, step(e)); }

export { Var, Lam, App, subst, step, normalize, checkConfluence, isNormal, eq };
