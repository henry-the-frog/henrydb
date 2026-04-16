/**
 * Functorial Semantics: Map between categories of types
 */
class Functor { constructor(name, map) { this.name = name; this.map = map; }
  fmap(f, x) { return this.map(f, x); }
}

const listFunctor = new Functor('List', (f, xs) => xs.map(f));
const maybeFunctor = new Functor('Maybe', (f, x) => x === null ? null : f(x));
const pairFunctor = new Functor('Pair', (f, [a, b]) => [f(a), b]);
const eitherFunctor = new Functor('Either', (f, x) => x.tag === 'Right' ? { tag: 'Right', value: f(x.value) } : x);
const constFunctor = new Functor('Const', (f, x) => x); // Ignores f
const idFunctor = new Functor('Id', (f, x) => f(x));

function checkFunctorLaw1(F, x) { return JSON.stringify(F.fmap(a => a, x)) === JSON.stringify(x); }
function checkFunctorLaw2(F, f, g, x) { return JSON.stringify(F.fmap(a => f(g(a)), x)) === JSON.stringify(F.fmap(f, F.fmap(g, x))); }

class NatTrans { constructor(name, transform) { this.name = name; this.transform = transform; }
  apply(x) { return this.transform(x); }
}

const headNat = new NatTrans('head: List → Maybe', xs => xs.length > 0 ? xs[0] : null);
const singletonNat = new NatTrans('singleton: Maybe → List', x => x === null ? [] : [x]);

export { Functor, listFunctor, maybeFunctor, pairFunctor, eitherFunctor, constFunctor, idFunctor, checkFunctorLaw1, checkFunctorLaw2, NatTrans, headNat, singletonNat };
