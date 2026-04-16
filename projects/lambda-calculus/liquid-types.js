/**
 * Liquid Types: Types refined with SMT predicates
 */
class LiquidType {
  constructor(base, pred) { this.base = base; this.pred = pred; }
  check(value) { return typeof value === this.base && this.pred(value); }
  toString() { return `{v:${this.base} | ${this.predStr || '...'}}`; }
}

function posInt() { return Object.assign(new LiquidType('number', v => Number.isInteger(v) && v > 0), { predStr: 'v > 0' }); }
function nat() { return Object.assign(new LiquidType('number', v => Number.isInteger(v) && v >= 0), { predStr: 'v >= 0' }); }
function range(lo, hi) { return Object.assign(new LiquidType('number', v => v >= lo && v <= hi), { predStr: `${lo} <= v <= ${hi}` }); }
function nonEmpty() { return Object.assign(new LiquidType('string', v => v.length > 0), { predStr: 'len(v) > 0' }); }
function sorted() { return Object.assign(new LiquidType('object', v => Array.isArray(v) && v.every((x,i) => i === 0 || v[i-1] <= x)), { predStr: 'sorted(v)' }); }

function subtype(t1, t2) {
  if (t1.base !== t2.base) return false;
  // Check: ∀v. t1.pred(v) ⟹ t2.pred(v)
  // Approximate with sampling
  const samples = t1.base === 'number' ? [-10,-1,0,1,2,5,10,100] : ['', 'a', 'hello'];
  return samples.every(s => !t1.check(s) || t2.check(s));
}

function meet(t1, t2) { return new LiquidType(t1.base, v => t1.pred(v) && t2.pred(v)); }
function join(t1, t2) { return new LiquidType(t1.base, v => t1.pred(v) || t2.pred(v)); }

export { LiquidType, posInt, nat, range, nonEmpty, sorted, subtype, meet, join };
