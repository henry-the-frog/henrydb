/**
 * Module #187: Weakening and Contraction — Structural rules
 */
function weaken(ctx, newVar, type) { return new Map([...ctx, [newVar, type]]); }
function contract(ctx, var1, var2) {
  if (!ctx.has(var1) || !ctx.has(var2)) throw new Error('Missing');
  if (ctx.get(var1) !== ctx.get(var2)) throw new Error('Type mismatch for contraction');
  const r = new Map(ctx); r.delete(var2); return r;
}
function exchange(ctx, v1, v2) {
  if (!ctx.has(v1) || !ctx.has(v2)) throw new Error('Missing');
  const r = new Map(ctx); const t1 = ctx.get(v1), t2 = ctx.get(v2);
  r.set(v1, t2); r.set(v2, t1); return r;
}

function checkWeakening(ctx1, ctx2) { for (const [k,v] of ctx1) { if (!ctx2.has(k) || ctx2.get(k) !== v) return false; } return true; }
function isLinear(usage) { return Object.values(usage).every(n => n === 1); }
function isAffine(usage) { return Object.values(usage).every(n => n <= 1); }
function isRelevant(usage) { return Object.values(usage).every(n => n >= 1); }

export { weaken, contract, exchange, checkWeakening, isLinear, isAffine, isRelevant };
