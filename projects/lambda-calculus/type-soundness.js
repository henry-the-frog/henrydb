/**
 * #189: Type Soundness — Progress + Preservation in one module
 */
const TInt = { tag:'TInt' }, TBool = { tag:'TBool' };
const TFun = (p,r) => ({ tag:'TFun', param:p, ret:r });

function typecheck(e, ctx = new Map()) {
  switch(e.tag) {
    case 'Num': return TInt;
    case 'Bool': return TBool;
    case 'Var': { const t = ctx.get(e.name); if (!t) throw new Error(`Unbound: ${e.name}`); return t; }
    case 'Add': { const l = typecheck(e.left, ctx), r = typecheck(e.right, ctx); if (l.tag!=='TInt'||r.tag!=='TInt') throw new Error('Add: need Int'); return TInt; }
    case 'If': { if (typecheck(e.cond,ctx).tag!=='TBool') throw new Error('If: need Bool'); const t = typecheck(e.then,ctx), f = typecheck(e.else,ctx); if (t.tag!==f.tag) throw new Error('If: branch mismatch'); return t; }
    case 'Lam': { const newCtx = new Map([...ctx,[e.var,e.paramType]]); return TFun(e.paramType, typecheck(e.body, newCtx)); }
    case 'App': { const fn = typecheck(e.fn,ctx); if(fn.tag!=='TFun') throw new Error('App: not function'); const arg = typecheck(e.arg,ctx); if(arg.tag!==fn.param.tag) throw new Error('App: arg mismatch'); return fn.ret; }
  }
}

function isValue(e) { return e.tag==='Num'||e.tag==='Bool'||e.tag==='Lam'; }
function step(e) {
  if(e.tag==='Add'&&e.left.tag==='Num'&&e.right.tag==='Num') return { tag:'Num', n: e.left.n+e.right.n };
  if(e.tag==='If'&&e.cond.tag==='Bool') return e.cond.b ? e.then : e.else;
  if(e.tag==='App'&&e.fn.tag==='Lam'&&isValue(e.arg)) return subst(e.fn.body,e.fn.var,e.arg);
  return null;
}
function subst(e,x,s){switch(e.tag){case'Var':return e.name===x?s:e;case'Lam':return e.var===x?e:{...e,body:subst(e.body,x,s)};case'App':return{...e,fn:subst(e.fn,x,s),arg:subst(e.arg,x,s)};case'Add':return{...e,left:subst(e.left,x,s),right:subst(e.right,x,s)};case'If':return{...e,cond:subst(e.cond,x,s),then:subst(e.then,x,s),else:subst(e.else,x,s)};default:return e;}}

// Progress: well-typed, non-value → can step
function checkProgress(e, ctx) { try { typecheck(e, ctx); } catch { return true; } return isValue(e) || step(e) !== null; }
// Preservation: step preserves type
function checkPreservation(e, ctx) {
  let t1; try { t1 = typecheck(e, ctx); } catch { return true; }
  const e2 = step(e); if (!e2) return true;
  try { const t2 = typecheck(e2, ctx); return t1.tag === t2.tag; } catch { return false; }
}

export { TInt, TBool, TFun, typecheck, isValue, step, checkProgress, checkPreservation };
