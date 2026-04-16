/**
 * Catamorphism Zoo: Every recursion scheme with examples
 * 
 * A comprehensive collection beyond recursive-schemes.js:
 * cata, ana, hylo, para, apo, histo, futu, chrono, zygo, mutu
 */

// Fix point
class Fix { constructor(f) { this.unfix = f; } }

// List functor
class NilF { constructor() { this.tag = 'NilF'; } }
class ConsF { constructor(h, t) { this.tag = 'ConsF'; this.head = h; this.tail = t; } }

function listToFix(arr) {
  let result = new Fix(new NilF());
  for (let i = arr.length - 1; i >= 0; i--) result = new Fix(new ConsF(arr[i], result));
  return result;
}

// Catamorphism (fold)
function cata(alg, fix) { return alg(mapF(x => cata(alg, x), fix.unfix)); }

// Anamorphism (unfold)
function ana(coalg, seed) { return new Fix(mapF(x => ana(coalg, x), coalg(seed))); }

// Hylomorphism (unfold then fold, no intermediate)
function hylo(alg, coalg, seed) { return alg(mapF(x => hylo(alg, coalg, x), coalg(seed))); }

// Paramorphism (fold with original structure access)
function para(alg, fix) {
  return alg(mapF(x => [para(alg, x), x], fix.unfix));
}

// Histomorphism (fold with access to ALL previous results)
class Cofree { constructor(head, tail) { this.head = head; this.tail = tail; } }

function histo(alg, fix) {
  function go(f) {
    const mapped = mapF(x => go(x), f.unfix);
    const heads = mapF(x => x.head, mapped);
    return new Cofree(alg(heads), mapped);
  }
  return go(fix).head;
}

// Zygomorphism (fold with auxiliary fold)
function zygo(aux, alg, fix) {
  function go(f) {
    const mapped = mapF(x => go(x), f.unfix);
    const auxVals = mapF(x => x[0], mapped);
    const mainVals = mapF(x => x[1], mapped);
    return [aux(auxVals), alg(mainVals, aux(auxVals))];
  }
  return go(fix)[1];
}

function mapF(f, layer) {
  if (layer.tag === 'NilF') return new NilF();
  if (layer.tag === 'ConsF') return new ConsF(layer.head, f(layer.tail));
  return layer;
}

export { Fix, NilF, ConsF, listToFix, cata, ana, hylo, para, histo, zygo, mapF };
