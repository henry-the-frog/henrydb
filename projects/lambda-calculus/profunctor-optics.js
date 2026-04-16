/**
 * Profunctor Optics: Optics via profunctors (van Laarhoven, Pickering et al.)
 * 
 * Lens: focus on a field
 * Prism: focus on a variant  
 * Traversal: focus on many
 * Iso: bidirectional transformation
 */

// Profunctor: contravariant in first arg, covariant in second
// dimap :: (a' → a) → (b → b') → p a b → p a' b'

class Lens {
  constructor(get, set) { this.get = get; this.set = set; }
  view(s) { return this.get(s); }
  over(f, s) { return this.set(f(this.get(s)), s); }
  modify(s, f) { return this.set(f(this.get(s)), s); }
}

class Prism {
  constructor(match, build) { this.match = match; this.build = build; }
  preview(s) { return this.match(s); } // Maybe<a>
  review(a) { return this.build(a); }
}

class Iso {
  constructor(to, from) { this.to = to; this.from = from; }
  view(s) { return this.to(s); }
  review(a) { return this.from(a); }
}

class Traversal {
  constructor(traverseFn) { this.traverse = traverseFn; } // (a → [b]) → s → [t]
  toList(s) { const result = []; this.traverse(a => { result.push(a); return a; }, s); return result; }
  over(f, s) { return this.traverse(f, s); }
}

// Lens composition
function composeLens(outer, inner) {
  return new Lens(
    s => inner.get(outer.get(s)),
    (a, s) => outer.set(inner.set(a, outer.get(s)), s)
  );
}

// Standard lenses
function prop(key) {
  return new Lens(
    obj => obj[key],
    (val, obj) => ({ ...obj, [key]: val })
  );
}

function index(i) {
  return new Lens(
    arr => arr[i],
    (val, arr) => { const a = [...arr]; a[i] = val; return a; }
  );
}

// Prism for tagged unions
function tagged(tag) {
  return new Prism(
    obj => obj.tag === tag ? obj.value : null,
    value => ({ tag, value })
  );
}

// Iso examples
const celsiusFahrenheit = new Iso(
  c => c * 9 / 5 + 32,
  f => (f - 32) * 5 / 9
);

const stringNumber = new Iso(
  s => Number(s),
  n => String(n)
);

export { Lens, Prism, Iso, Traversal, composeLens, prop, index, tagged, celsiusFahrenheit, stringNumber };
