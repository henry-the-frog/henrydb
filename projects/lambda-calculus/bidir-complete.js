/**
 * Bidirectional Type Checking (Complete): Full check/infer with annotations
 */
class TInt { constructor() { this.tag='TInt'; } toString() { return 'Int'; } }
class TBool { constructor() { this.tag='TBool'; } toString() { return 'Bool'; } }
class TFun { constructor(p,r) { this.tag='TFun'; this.param=p; this.ret=r; } toString() { return `(${this.param} → ${this.ret})`; } }

function infer(expr, ctx) {
  switch(expr.tag) {
    case 'ENum': return new TInt();
    case 'EBool': return new TBool();
    case 'EVar': { const t = ctx.get(expr.name); if (!t) throw new Error(`Unbound: ${expr.name}`); return t; }
    case 'EApp': {
      const fnTy = infer(expr.fn, ctx);
      if (fnTy.tag !== 'TFun') throw new Error('Expected function type');
      check(expr.arg, fnTy.param, ctx);
      return fnTy.ret;
    }
    case 'EAnn': { check(expr.expr, expr.type, ctx); return expr.type; }
    default: throw new Error(`Cannot infer: ${expr.tag}`);
  }
}

function check(expr, expected, ctx) {
  if (expr.tag === 'ELam' && expected.tag === 'TFun') {
    check(expr.body, expected.ret, new Map([...ctx, [expr.var, expected.param]]));
    return;
  }
  if (expr.tag === 'EIf') {
    check(expr.cond, new TBool(), ctx);
    check(expr.then, expected, ctx);
    check(expr.else, expected, ctx);
    return;
  }
  const inferred = infer(expr, ctx);
  if (inferred.toString() !== expected.toString()) throw new Error(`Expected ${expected}, got ${inferred}`);
}

const ENum = n => ({ tag:'ENum', n }); const EBool = b => ({ tag:'EBool', b }); const EVar = n => ({ tag:'EVar', name:n });
const ELam = (v,b) => ({ tag:'ELam', var:v, body:b }); const EApp = (f,a) => ({ tag:'EApp', fn:f, arg:a });
const EAnn = (e,t) => ({ tag:'EAnn', expr:e, type:t }); const EIf = (c,t,f) => ({ tag:'EIf', cond:c, then:t, else:f });

export { TInt, TBool, TFun, infer, check, ENum, EBool, EVar, ELam, EApp, EAnn, EIf };
