/**
 * 🎉 Module #170: Definitional Interpreters — Interpreters as denotational semantics
 * Reynolds (1972): write interpreters that ARE the semantics.
 */

// Direct style interpreter (the denotation of an expression IS a JS value)
function evalDirect(expr, env = new Map()) {
  switch(expr.tag) {
    case 'Num': return expr.n;
    case 'Bool': return expr.b;
    case 'Var': return env.get(expr.name);
    case 'Lam': return arg => evalDirect(expr.body, new Map([...env, [expr.var, arg]]));
    case 'App': return evalDirect(expr.fn, env)(evalDirect(expr.arg, env));
    case 'Add': return evalDirect(expr.left, env) + evalDirect(expr.right, env);
    case 'If': return evalDirect(expr.cond, env) ? evalDirect(expr.then, env) : evalDirect(expr.else, env);
    case 'Let': return evalDirect(expr.body, new Map([...env, [expr.var, evalDirect(expr.init, env)]]));
    case 'Letrec': {
      const newEnv = new Map(env);
      const closure = arg => evalDirect(expr.fnBody, new Map([...newEnv, [expr.fnParam, arg]]));
      newEnv.set(expr.fnName, closure);
      return evalDirect(expr.body, newEnv);
    }
  }
}

// Monadic style (same semantics, explicit effects)
function evalMonadic(expr, env = new Map()) {
  switch(expr.tag) {
    case 'Num': return { value: expr.n, effects: [] };
    case 'Bool': return { value: expr.b, effects: [] };
    case 'Var': return { value: env.get(expr.name), effects: [] };
    case 'Add': {
      const l = evalMonadic(expr.left, env), r = evalMonadic(expr.right, env);
      return { value: l.value + r.value, effects: [...l.effects, ...r.effects] };
    }
    case 'Print': {
      const v = evalMonadic(expr.expr, env);
      return { value: v.value, effects: [...v.effects, { type: 'print', value: v.value }] };
    }
    default: return evalDirect(expr, env) !== undefined ? { value: evalDirect(expr, env), effects: [] } : { value: null, effects: [] };
  }
}

// Constructors
const Num = n => ({ tag: 'Num', n });
const Bool = b => ({ tag: 'Bool', b });
const Var = name => ({ tag: 'Var', name });
const Lam = (v, body) => ({ tag: 'Lam', var: v, body });
const App = (fn, arg) => ({ tag: 'App', fn, arg });
const Add = (l, r) => ({ tag: 'Add', left: l, right: r });
const If = (c, t, f) => ({ tag: 'If', cond: c, then: t, else: f });
const Let = (v, init, body) => ({ tag: 'Let', var: v, init, body });
const Letrec = (fnName, fnParam, fnBody, body) => ({ tag: 'Letrec', fnName, fnParam, fnBody, body });
const Print = expr => ({ tag: 'Print', expr });

export { evalDirect, evalMonadic, Num, Bool, Var, Lam, App, Add, If, Let, Letrec, Print };
