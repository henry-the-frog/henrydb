/**
 * Bidirectional Elaboration: Surface syntax → Core language
 * 
 * Transform high-level, user-friendly syntax into a well-typed core language.
 * Surface: may have holes, implicit arguments, operator overloading
 * Core: fully explicit, annotated, desugared
 */

// Surface language
class SVar { constructor(name) { this.tag = 'SVar'; this.name = name; } }
class SNum { constructor(n) { this.tag = 'SNum'; this.n = n; } }
class SApp { constructor(fn, arg) { this.tag = 'SApp'; this.fn = fn; this.arg = arg; } }
class SLam { constructor(v, body) { this.tag = 'SLam'; this.var = v; this.body = body; } }
class SLet { constructor(v, init, body) { this.tag = 'SLet'; this.var = v; this.init = init; this.body = body; } }
class SHole { constructor() { this.tag = 'SHole'; } } // _ (to be inferred)
class SIf { constructor(c, t, f) { this.tag = 'SIf'; this.cond = c; this.then = t; this.else = f; } }

// Core language (fully annotated)
class CVar { constructor(name, type) { this.tag = 'CVar'; this.name = name; this.type = type; } }
class CNum { constructor(n) { this.tag = 'CNum'; this.n = n; this.type = 'Int'; } }
class CBool { constructor(b) { this.tag = 'CBool'; this.b = b; this.type = 'Bool'; } }
class CApp { constructor(fn, arg, retType) { this.tag = 'CApp'; this.fn = fn; this.arg = arg; this.type = retType; } }
class CLam { constructor(v, paramType, body) { this.tag = 'CLam'; this.var = v; this.paramType = paramType; this.body = body; this.type = `${paramType} → ${body.type}`; } }
class CLet { constructor(v, init, body) { this.tag = 'CLet'; this.var = v; this.init = init; this.body = body; this.type = body.type; } }
class CIf { constructor(c, t, f, type) { this.tag = 'CIf'; this.cond = c; this.then = t; this.else = f; this.type = type; } }

// Elaboration
function elaborate(surface, env = new Map(), expected = null) {
  switch (surface.tag) {
    case 'SNum': return new CNum(surface.n);
    case 'SVar': {
      const type = env.get(surface.name) || 'unknown';
      return new CVar(surface.name, type);
    }
    case 'SLam': {
      const paramType = expected && expected.includes('→') ? expected.split(' → ')[0] : 'unknown';
      const retType = expected && expected.includes('→') ? expected.split(' → ').slice(1).join(' → ') : null;
      const newEnv = new Map([...env, [surface.var, paramType]]);
      const body = elaborate(surface.body, newEnv, retType);
      return new CLam(surface.var, paramType, body);
    }
    case 'SApp': {
      const fn = elaborate(surface.fn, env);
      const arg = elaborate(surface.arg, env);
      const retType = fn.type.includes('→') ? fn.type.split(' → ').slice(1).join(' → ') : 'unknown';
      return new CApp(fn, arg, retType);
    }
    case 'SLet': {
      const init = elaborate(surface.init, env);
      const newEnv = new Map([...env, [surface.var, init.type]]);
      const body = elaborate(surface.body, newEnv, expected);
      return new CLet(surface.var, init, body);
    }
    case 'SHole': return { tag: 'CHole', type: expected || '?', message: `Hole: expected ${expected || '?'}` };
    case 'SIf': {
      const cond = elaborate(surface.cond, env, 'Bool');
      const then_ = elaborate(surface.then, env, expected);
      const else_ = elaborate(surface.else, env, expected);
      return new CIf(cond, then_, else_, then_.type);
    }
    default: throw new Error(`Can't elaborate: ${surface.tag}`);
  }
}

// Find all holes
function findHoles(core) {
  if (core.tag === 'CHole') return [core];
  const holes = [];
  for (const key of Object.keys(core)) {
    if (core[key] && typeof core[key] === 'object' && core[key].tag) {
      holes.push(...findHoles(core[key]));
    }
  }
  return holes;
}

export { SVar, SNum, SApp, SLam, SLet, SHole, SIf, CVar, CNum, CApp, CLam, CLet, CIf, elaborate, findHoles };
