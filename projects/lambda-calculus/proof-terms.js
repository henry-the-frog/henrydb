/**
 * Proof Terms: Curry-Howard witnesses
 * 
 * Every proof corresponds to a program. A proof term is the actual
 * program that witnesses a proposition.
 * 
 * A → B is a function
 * A ∧ B is a pair
 * A ∨ B is a tagged union (either)
 */

// Proof terms
class PVar { constructor(name) { this.tag = 'PVar'; this.name = name; } toString() { return this.name; } }
class PLam { constructor(v, body) { this.tag = 'PLam'; this.var = v; this.body = body; } toString() { return `(λ${this.var}. ${this.body})`; } }
class PApp { constructor(fn, arg) { this.tag = 'PApp'; this.fn = fn; this.arg = arg; } toString() { return `(${this.fn} ${this.arg})`; } }
class PPair { constructor(fst, snd) { this.tag = 'PPair'; this.fst = fst; this.snd = snd; } toString() { return `⟨${this.fst}, ${this.snd}⟩`; } }
class PFst { constructor(pair) { this.tag = 'PFst'; this.pair = pair; } toString() { return `π₁(${this.pair})`; } }
class PSnd { constructor(pair) { this.tag = 'PSnd'; this.pair = pair; } toString() { return `π₂(${this.pair})`; } }
class PInl { constructor(val) { this.tag = 'PInl'; this.val = val; } toString() { return `inl(${this.val})`; } }
class PInr { constructor(val) { this.tag = 'PInr'; this.val = val; } toString() { return `inr(${this.val})`; } }
class PCase { constructor(scrut, l, r) { this.tag = 'PCase'; this.scrutinee = scrut; this.left = l; this.right = r; } }

// Type check proof terms against propositions
function checkProof(term, prop, ctx = new Map()) {
  switch (term.tag) {
    case 'PVar': return ctx.get(term.name) === prop;
    case 'PLam': {
      if (!prop.includes('→')) return false;
      const i = findArrow(prop);
      const paramType = prop.substring(0, i).trim();
      const retType = prop.substring(i + 1).trim();
      return checkProof(term.body, retType, new Map([...ctx, [term.var, paramType]]));
    }
    case 'PPair': {
      if (!prop.includes('∧')) return false;
      const [l, r] = prop.split(' ∧ ');
      return checkProof(term.fst, l.trim(), ctx) && checkProof(term.snd, r.trim(), ctx);
    }
    case 'PInl': {
      if (!prop.includes('∨')) return false;
      const [l] = prop.split(' ∨ ');
      return checkProof(term.val, l.trim(), ctx);
    }
    case 'PInr': {
      if (!prop.includes('∨')) return false;
      const parts = prop.split(' ∨ ');
      return checkProof(term.val, parts.slice(1).join(' ∨ ').trim(), ctx);
    }
    default: return false;
  }
}

function findArrow(s) {
  let depth = 0;
  for (let i = 0; i < s.length - 1; i++) {
    if (s[i] === '(') depth++;
    if (s[i] === ')') depth--;
    if (depth === 0 && s[i] === '→') return i;
  }
  return s.indexOf('→');
}

// Eval proof term (extract computational content)
function evalProof(term, env = new Map()) {
  switch (term.tag) {
    case 'PVar': return env.get(term.name);
    case 'PLam': return arg => evalProof(term.body, new Map([...env, [term.var, arg]]));
    case 'PApp': return evalProof(term.fn, env)(evalProof(term.arg, env));
    case 'PPair': return [evalProof(term.fst, env), evalProof(term.snd, env)];
    case 'PFst': return evalProof(term.pair, env)[0];
    case 'PSnd': return evalProof(term.pair, env)[1];
    case 'PInl': return { tag: 'Left', value: evalProof(term.val, env) };
    case 'PInr': return { tag: 'Right', value: evalProof(term.val, env) };
    default: return null;
  }
}

export { PVar, PLam, PApp, PPair, PFst, PSnd, PInl, PInr, PCase, checkProof, evalProof };
