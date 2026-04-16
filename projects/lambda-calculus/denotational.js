/**
 * 🎉🎉🎉 MODULE #180: Denotational Semantics — Mathematical meaning of programs 🎉🎉🎉
 */
function denote(expr, env = new Map()) {
  switch(expr.tag) {
    case 'Num': return expr.n;
    case 'Bool': return expr.b;
    case 'Var': return env.get(expr.name);
    case 'Add': return denote(expr.left, env) + denote(expr.right, env);
    case 'Mul': return denote(expr.left, env) * denote(expr.right, env);
    case 'Lam': return arg => denote(expr.body, new Map([...env, [expr.var, arg]]));
    case 'App': return denote(expr.fn, env)(denote(expr.arg, env));
    case 'Let': return denote(expr.body, new Map([...env, [expr.var, denote(expr.init, env)]]));
    case 'If': return denote(expr.cond, env) ? denote(expr.then, env) : denote(expr.else, env);
    case 'Fix': { const f = denote(expr.fn, env); return fix(f); }
    case 'Seq': { denote(expr.first, env); return denote(expr.second, env); }
    case 'Unit': return null;
  }
}

function fix(f, n = 100) { let x = null; for (let i = 0; i < n; i++) x = f(x); return x; }

// Semantic domains
const Bot = Symbol('⊥'); // Bottom (undefined/diverge)
function lift(f) { return x => x === Bot ? Bot : f(x); }
function strict(f) { return x => x === Bot ? Bot : f(x); }

const N = n => ({ tag:'Num', n }); const B = b => ({ tag:'Bool', b }); const V = n => ({ tag:'Var', name:n });
const Add = (l,r) => ({ tag:'Add', left:l, right:r }); const Mul = (l,r) => ({ tag:'Mul', left:l, right:r });
const Lam = (v,b) => ({ tag:'Lam', var:v, body:b }); const App = (f,a) => ({ tag:'App', fn:f, arg:a });
const Let = (v,i,b) => ({ tag:'Let', var:v, init:i, body:b }); const If = (c,t,f) => ({ tag:'If', cond:c, then:t, else:f });
const Seq = (a,b) => ({ tag:'Seq', first:a, second:b }); const Unit = () => ({ tag:'Unit' });

export { denote, fix, Bot, lift, strict, N, B, V, Add, Mul, Lam, App, Let, If, Seq, Unit };
