/**
 * Continuation Semantics: Denotational semantics via continuations
 * 
 * Instead of computing a value, compute "what happens next" (the continuation).
 * Every expression takes a continuation k and calls k with its result.
 */

const identity = x => x;

function evalCont(expr, env = new Map(), k = identity) {
  switch (expr.tag) {
    case 'Num': return k(expr.n);
    case 'Var': {
      const v = env.get(expr.name);
      if (v === undefined) throw new Error(`Unbound: ${expr.name}`);
      return k(v);
    }
    case 'Add': return evalCont(expr.left, env, l => evalCont(expr.right, env, r => k(l + r)));
    case 'Mul': return evalCont(expr.left, env, l => evalCont(expr.right, env, r => k(l * r)));
    case 'Lam': return k({ tag: 'Closure', var: expr.var, body: expr.body, env: new Map(env) });
    case 'App': return evalCont(expr.fn, env, fn => {
      if (fn.tag !== 'Closure') throw new Error('Not a function');
      return evalCont(expr.arg, env, arg => {
        return evalCont(fn.body, new Map([...fn.env, [fn.var, arg]]), k);
      });
    });
    case 'CallCC': {
      // Call with current continuation: callcc(f) applies f to k
      return evalCont(expr.fn, env, fn => {
        if (fn.tag !== 'Closure') throw new Error('CallCC: not a function');
        const reifiedK = { tag: 'Continuation', k };
        return evalCont(fn.body, new Map([...fn.env, [fn.var, reifiedK]]), k);
      });
    }
    case 'Throw': {
      return evalCont(expr.cont, env, cont => {
        return evalCont(expr.val, env, val => {
          if (cont.tag !== 'Continuation') throw new Error('Not a continuation');
          return cont.k(val); // Jump to captured continuation
        });
      });
    }
    case 'Let': return evalCont(expr.init, env, val => evalCont(expr.body, new Map([...env, [expr.var, val]]), k));
    case 'If0': return evalCont(expr.cond, env, c => c === 0 ? evalCont(expr.then, env, k) : evalCont(expr.else, env, k));
    default: throw new Error(`Unknown: ${expr.tag}`);
  }
}

// Expression constructors
class Num { constructor(n) { this.tag = 'Num'; this.n = n; } }
class Var { constructor(name) { this.tag = 'Var'; this.name = name; } }
class Add { constructor(l, r) { this.tag = 'Add'; this.left = l; this.right = r; } }
class Mul { constructor(l, r) { this.tag = 'Mul'; this.left = l; this.right = r; } }
class Lam { constructor(v, body) { this.tag = 'Lam'; this.var = v; this.body = body; } }
class App { constructor(fn, arg) { this.tag = 'App'; this.fn = fn; this.arg = arg; } }
class Let { constructor(v, init, body) { this.tag = 'Let'; this.var = v; this.init = init; this.body = body; } }
class If0 { constructor(c, t, f) { this.tag = 'If0'; this.cond = c; this.then = t; this.else = f; } }
class CallCC { constructor(fn) { this.tag = 'CallCC'; this.fn = fn; } }
class Throw { constructor(cont, val) { this.tag = 'Throw'; this.cont = cont; this.val = val; } }

function run(expr) { return evalCont(expr); }

export { Num, Var, Add, Mul, Lam, App, Let, If0, CallCC, Throw, evalCont, run };
