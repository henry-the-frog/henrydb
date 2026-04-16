/**
 * Recursive Schemes: Structured recursion patterns
 * 
 * Beyond catamorphism (fold) and anamorphism (unfold):
 * - Paramorphism: fold with access to original structure
 * - Apomorphism: unfold with early termination
 * - Hylomorphism: unfold then fold (no intermediate structure)
 * - Histomorphism: fold with access to history
 * - Futumorphism: unfold with lookahead
 */

// Fix point
class Fix { constructor(layer) { this.layer = layer; } }
const fix = layer => new Fix(layer);
const unfix = term => term.layer;

// List functor
class NilF { constructor() { this.tag = 'NilF'; } }
class ConsF { constructor(head, tail) { this.tag = 'ConsF'; this.head = head; this.tail = tail; } }

function fmap(f, layer) {
  if (layer.tag === 'NilF') return layer;
  if (layer.tag === 'ConsF') return new ConsF(layer.head, f(layer.tail));
  return layer;
}

// Catamorphism (fold)
function cata(alg, term) {
  return alg(fmap(t => cata(alg, t), unfix(term)));
}

// Anamorphism (unfold)
function ana(coalg, seed) {
  return fix(fmap(s => ana(coalg, s), coalg(seed)));
}

// Hylomorphism (unfold then fold, no intermediate structure)
function hylo(alg, coalg, seed) {
  return alg(fmap(s => hylo(alg, coalg, s), coalg(seed)));
}

// Paramorphism (fold with original structure access)
function para(alg, term) {
  return alg(fmap(t => [para(alg, t), t], unfix(term)));
}

// Apomorphism (unfold with early termination)
function apo(coalg, seed) {
  const layer = coalg(seed);
  return fix(fmap(either => {
    if (either.tag === 'Left') return either.value; // Already a Fix, stop
    return apo(coalg, either.value);                 // Continue unfolding
  }, layer));
}

function Left(v) { return { tag: 'Left', value: v }; }
function Right(v) { return { tag: 'Right', value: v }; }

// Helpers: build/deconstruct lists
function listToFix(arr) {
  let r = fix(new NilF());
  for (let i = arr.length - 1; i >= 0; i--) r = fix(new ConsF(arr[i], r));
  return r;
}

function fixToList(term) {
  return cata(layer => layer.tag === 'NilF' ? [] : [layer.head, ...layer.tail], term);
}

export { Fix, fix, unfix, NilF, ConsF, fmap, cata, ana, hylo, para, apo, Left, Right, listToFix, fixToList };
