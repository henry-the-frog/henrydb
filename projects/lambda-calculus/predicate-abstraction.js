/**
 * Predicate Abstraction: Abstract with logical predicates
 */
class AbstractDomain {
  constructor(predicates) { this.predicates = predicates; }
  abstract(value) {
    const result = {};
    for (const [name, pred] of Object.entries(this.predicates)) result[name] = pred(value);
    return result;
  }
  refine(abstract, predName, value) { return { ...abstract, [predName]: value }; }
  isBottom(abstract) { return Object.values(abstract).every(v => v === false); }
  join(a, b) { const r = {}; for (const k of Object.keys(a)) r[k] = a[k] || b[k]; return r; }
  meet(a, b) { const r = {}; for (const k of Object.keys(a)) r[k] = a[k] && b[k]; return r; }
  implies(a, b) { return Object.keys(b).every(k => !b[k] || a[k]); }
}

const signDomain = new AbstractDomain({ positive: v => v > 0, zero: v => v === 0, negative: v => v < 0 });
const boundsDomain = new AbstractDomain({ small: v => Math.abs(v) < 100, large: v => Math.abs(v) >= 100, even: v => v % 2 === 0 });

export { AbstractDomain, signDomain, boundsDomain };
