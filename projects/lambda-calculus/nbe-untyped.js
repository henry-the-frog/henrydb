/**
 * Normalization by Evaluation (extended): Untyped NbE
 */

class VLam { constructor(name, body) { this.tag = 'VLam'; this.name = name; this.body = body; } }
class VNeutral { constructor(ne) { this.tag = 'VNeutral'; this.ne = ne; } }
class NVar { constructor(name) { this.tag = 'NVar'; this.name = name; } }
class NApp { constructor(ne, nf) { this.tag = 'NApp'; this.ne = ne; this.nf = nf; } }

class Var { constructor(n) { this.tag='Var'; this.name=n; } }
class Lam { constructor(v,b) { this.tag='Lam'; this.var=v; this.body=b; } }
class App { constructor(f,a) { this.tag='App'; this.fn=f; this.arg=a; } }

function evaluate(expr, env = new Map()) {
  switch (expr.tag) {
    case 'Var': return env.has(expr.name) ? env.get(expr.name) : new VNeutral(new NVar(expr.name));
    case 'Lam': return new VLam(expr.var, arg => evaluate(expr.body, new Map([...env, [expr.var, arg]])));
    case 'App': return doApp(evaluate(expr.fn, env), evaluate(expr.arg, env));
  }
}

function doApp(fn, arg) {
  if (fn.tag === 'VLam') return fn.body(arg);
  if (fn.tag === 'VNeutral') return new VNeutral(new NApp(fn.ne, arg));
  throw new Error(`Cannot apply ${fn.tag}`);
}

let _freshCounter = 0;
function freshName(base = 'x') { return `${base}${_freshCounter++}`; }
function resetFresh() { _freshCounter = 0; }

function readback(val) {
  switch (val.tag) {
    case 'VLam': {
      const name = freshName(val.name);
      const body = readback(val.body(new VNeutral(new NVar(name))));
      return new Lam(name, body);
    }
    case 'VNeutral': return readbackNeutral(val.ne);
  }
}

function readbackNeutral(ne) {
  switch (ne.tag) {
    case 'NVar': return new Var(ne.name);
    case 'NApp': return new App(readbackNeutral(ne.ne), readback(ne.nf));
  }
}

function nbe(expr) { resetFresh(); return readback(evaluate(expr)); }
function exprToString(e) {
  switch(e.tag) {
    case 'Var': return e.name;
    case 'Lam': return `(λ${e.var}.${exprToString(e.body)})`;
    case 'App': return `(${exprToString(e.fn)} ${exprToString(e.arg)})`;
  }
}

export { Var, Lam, App, evaluate, readback, nbe, exprToString, resetFresh };
