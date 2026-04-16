/**
 * Multi-Stage Programming: Staged compilation with quote/splice
 * 
 * Build code at one stage, run it at the next.
 * <e>: quote (delay evaluation to next stage)
 * ~e: splice (run a computation at the current stage, insert result)
 * run e: execute staged code
 */

class Code {
  constructor(ast) { this.tag = 'Code'; this.ast = ast; }
  toString() { return `<${this.ast}>`; }
}

class CNum { constructor(n) { this.tag = 'CNum'; this.n = n; } toString() { return `${this.n}`; } }
class CVar { constructor(name) { this.tag = 'CVar'; this.name = name; } toString() { return this.name; } }
class CAdd { constructor(l, r) { this.tag = 'CAdd'; this.left = l; this.right = r; } toString() { return `(${this.left} + ${this.right})`; } }
class CMul { constructor(l, r) { this.tag = 'CMul'; this.left = l; this.right = r; } toString() { return `(${this.left} * ${this.right})`; } }
class CLam { constructor(v, body) { this.tag = 'CLam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}. ${this.body})`; } }
class CApp { constructor(fn, arg) { this.tag = 'CApp'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class CLet { constructor(v, init, body) { this.tag = 'CLet'; this.var = v; this.init = init; this.body = body; } }

// Quote: lift value into code
function quote(value) {
  if (typeof value === 'number') return new Code(new CNum(value));
  if (value instanceof Code) return value;
  throw new Error(`Can't quote: ${value}`);
}

// Splice: insert code into larger code
function splice(code) {
  if (code instanceof Code) return code.ast;
  throw new Error('Splice: not code');
}

// Run: evaluate staged code
function run(code, env = new Map()) {
  const ast = code instanceof Code ? code.ast : code;
  return evalAST(ast, env);
}

function evalAST(ast, env) {
  switch (ast.tag) {
    case 'CNum': return ast.n;
    case 'CVar': { const v = env.get(ast.name); if (v === undefined) throw new Error(`Unbound: ${ast.name}`); return v; }
    case 'CAdd': return evalAST(ast.left, env) + evalAST(ast.right, env);
    case 'CMul': return evalAST(ast.left, env) * evalAST(ast.right, env);
    case 'CLam': return arg => evalAST(ast.body, new Map([...env, [ast.var, arg]]));
    case 'CApp': return evalAST(ast.fn, env)(evalAST(ast.arg, env));
    case 'CLet': return evalAST(ast.body, new Map([...env, [ast.var, evalAST(ast.init, env)]]));
    default: throw new Error(`Unknown AST: ${ast.tag}`);
  }
}

// Stage: specialize a function for known static inputs
function stage(fn, staticArgs) {
  const code = fn(...staticArgs.map(a => new Code(new CNum(a))));
  return code;
}

// Power function: stage for known exponent
function powerStaged(n) {
  if (n === 0) return new Code(new CNum(1));
  if (n === 1) return new Code(new CVar('x'));
  return new Code(new CMul(splice(powerStaged(n - 1)), new CVar('x')));
}

export { Code, CNum, CVar, CAdd, CMul, CLam, CApp, CLet, quote, splice, run, stage, powerStaged };
