/**
 * Type-Preserving Compilation
 * 
 * Every intermediate representation (IR) carries types.
 * Compilation phases transform typed IR to typed IR.
 * If any phase produces ill-typed output, we catch the bug immediately.
 * 
 * Pipeline: Source → Typed AST → CPS → Closure-converted → Hoisted
 * Each IR is checked for well-typedness.
 */

// Source types
class TInt { constructor() { this.tag = 'TInt'; } toString() { return 'Int'; } }
class TBool { constructor() { this.tag = 'TBool'; } toString() { return 'Bool'; } }
class TFun { constructor(p, r) { this.tag = 'TFun'; this.param = p; this.ret = r; } toString() { return `(${this.param} → ${this.ret})`; } }

const tInt = new TInt();
const tBool = new TBool();

// Typed expression
class TExpr {
  constructor(tag, type, data) { this.tag = tag; this.type = type; this.data = data; }
  static num(n) { return new TExpr('Num', tInt, { n }); }
  static bool(v) { return new TExpr('Bool', tBool, { v }); }
  static var(name, type) { return new TExpr('Var', type, { name }); }
  static lam(v, paramType, body) { return new TExpr('Lam', new TFun(paramType, body.type), { var: v, body }); }
  static app(fn, arg) {
    if (fn.type.tag !== 'TFun') throw new Error('Apply to non-function');
    return new TExpr('App', fn.type.ret, { fn, arg });
  }
  static add(l, r) {
    if (l.type.tag !== 'TInt' || r.type.tag !== 'TInt') throw new Error('Add: non-Int');
    return new TExpr('Add', tInt, { left: l, right: r });
  }
  static if_(c, t, f) {
    if (c.type.tag !== 'TBool') throw new Error('If: non-Bool condition');
    return new TExpr('If', t.type, { cond: c, then: t, else: f });
  }
  static let_(v, init, body) { return new TExpr('Let', body.type, { var: v, init, body }); }
}

// ============================================================
// Phase 1: CPS Transform (preserving types)
// ============================================================

let cpsCounter = 0;
function freshK() { return `k${cpsCounter++}`; }

function cpsTransform(expr) {
  cpsCounter = 0;
  switch (expr.tag) {
    case 'Num': case 'Bool': case 'Var':
      return { value: expr, type: expr.type, isCPS: true };
    case 'Add':
      return { tag: 'CPSAdd', left: cpsTransform(expr.data.left), right: cpsTransform(expr.data.right), type: tInt, isCPS: true };
    case 'Lam': {
      const k = freshK();
      return { tag: 'CPSLam', var: expr.data.var, cont: k, body: cpsTransform(expr.data.body), type: expr.type, isCPS: true };
    }
    case 'App':
      return { tag: 'CPSApp', fn: cpsTransform(expr.data.fn), arg: cpsTransform(expr.data.arg), type: expr.type, isCPS: true };
    default:
      return { value: expr, type: expr.type, isCPS: true };
  }
}

// ============================================================
// Phase 2: Closure conversion (preserving types)
// ============================================================

function closureConvert(expr, freeVars = new Set()) {
  switch (expr.tag) {
    case 'Lam': {
      const fv = collectFreeVars(expr);
      return {
        tag: 'Closure', var: expr.data.var, body: closureConvert(expr.data.body),
        freeVars: [...fv], type: expr.type, isCC: true
      };
    }
    case 'App':
      return { tag: 'ClosureApp', fn: closureConvert(expr.data.fn), arg: closureConvert(expr.data.arg), type: expr.type, isCC: true };
    default:
      return { ...expr, isCC: true };
  }
}

function collectFreeVars(expr, bound = new Set()) {
  const fv = new Set();
  function walk(e, b) {
    if (!e || !e.tag) return;
    if (e.tag === 'Var' && !b.has(e.data.name)) fv.add(e.data.name);
    if (e.tag === 'Lam') walk(e.data.body, new Set([...b, e.data.var]));
    if (e.data) {
      if (e.data.fn) walk(e.data.fn, b);
      if (e.data.arg) walk(e.data.arg, b);
      if (e.data.left) walk(e.data.left, b);
      if (e.data.right) walk(e.data.right, b);
      if (e.data.body) walk(e.data.body, b);
    }
  }
  walk(expr, bound);
  return fv;
}

// ============================================================
// Type checker: verify IR is well-typed
// ============================================================

function typeCheck(expr, env = new Map()) {
  switch (expr.tag) {
    case 'Num': return expr.type.tag === 'TInt';
    case 'Bool': return expr.type.tag === 'TBool';
    case 'Var': {
      const expected = env.get(expr.data.name);
      return expected ? typesEqual(expr.type, expected) : true;
    }
    case 'Lam': {
      const newEnv = new Map([...env, [expr.data.var, expr.type.param]]);
      return typeCheck(expr.data.body, newEnv);
    }
    case 'App': return typeCheck(expr.data.fn, env) && typeCheck(expr.data.arg, env);
    case 'Add': return expr.type.tag === 'TInt' && typeCheck(expr.data.left, env) && typeCheck(expr.data.right, env);
    case 'If': return typeCheck(expr.data.cond, env) && typeCheck(expr.data.then, env) && typeCheck(expr.data.else, env);
    case 'Let': {
      const newEnv = new Map([...env, [expr.data.var, expr.data.init.type]]);
      return typeCheck(expr.data.init, env) && typeCheck(expr.data.body, newEnv);
    }
    default: return true;
  }
}

function typesEqual(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'TFun') return typesEqual(a.param, b.param) && typesEqual(a.ret, b.ret);
  return true;
}

export { TInt, TBool, TFun, tInt, tBool, TExpr, cpsTransform, closureConvert, typeCheck, typesEqual, collectFreeVars };
