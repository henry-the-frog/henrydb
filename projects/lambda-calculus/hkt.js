/**
 * Higher-Kinded Types (HKT) and Typeclasses
 * 
 * Enables generic programming over type constructors.
 * Haskell's Functor, Applicative, Monad hierarchy.
 * 
 * Kind system:
 *   ★         — concrete types (Int, Bool)
 *   ★ → ★     — type constructors (List, Maybe, IO)
 *   ★ → ★ → ★ — binary type constructors (Either, Pair)
 *   (★ → ★) → ★ — higher-order (takes a type constructor)
 * 
 * Typeclasses:
 *   Functor f     : fmap :: (a → b) → f a → f b
 *   Applicative f : pure :: a → f a, ap :: f (a → b) → f a → f b
 *   Monad f       : return :: a → f a, bind :: f a → (a → f b) → f b
 */

// ============================================================
// Kinds
// ============================================================

class KStar {
  constructor() { this.tag = 'KStar'; }
  toString() { return '★'; }
}

class KArrow {
  constructor(from, to) { this.tag = 'KArrow'; this.from = from; this.to = to; }
  toString() { return `(${this.from} → ${this.to})`; }
}

const kStar = new KStar();
const kStarToStar = new KArrow(kStar, kStar);
const kStarStarToStar = new KArrow(kStar, new KArrow(kStar, kStar));

function kindEquals(k1, k2) {
  if (k1.tag !== k2.tag) return false;
  if (k1.tag === 'KStar') return true;
  return kindEquals(k1.from, k2.from) && kindEquals(k1.to, k2.to);
}

// ============================================================
// Type Constructors
// ============================================================

class TCon {
  constructor(name, kind) { this.tag = 'TCon'; this.name = name; this.kind = kind; }
  toString() { return this.name; }
}

class TApp {
  constructor(con, arg) { this.tag = 'TApp'; this.con = con; this.arg = arg; }
  toString() { return `${this.con} ${this.arg}`; }
}

class TVar {
  constructor(name, kind = kStar) { this.tag = 'TVar'; this.name = name; this.kind = kind; }
  toString() { return this.name; }
}

class TArrow {
  constructor(from, to) { this.tag = 'TArrow'; this.from = from; this.to = to; }
  toString() { return `(${this.from} → ${this.to})`; }
}

// Built-in type constructors
const tInt = new TCon('Int', kStar);
const tBool = new TCon('Bool', kStar);
const tStr = new TCon('String', kStar);
const tList = new TCon('List', kStarToStar);
const tMaybe = new TCon('Maybe', kStarToStar);
const tIO = new TCon('IO', kStarToStar);
const tEither = new TCon('Either', kStarStarToStar);
const tPair = new TCon('Pair', kStarStarToStar);

// Type applications
function listOf(t) { return new TApp(tList, t); }
function maybeOf(t) { return new TApp(tMaybe, t); }
function ioOf(t) { return new TApp(tIO, t); }
function eitherOf(l, r) { return new TApp(new TApp(tEither, l), r); }
function pairOf(a, b) { return new TApp(new TApp(tPair, a), b); }

// ============================================================
// Kind Checking
// ============================================================

function inferKind(type, env = new Map()) {
  switch (type.tag) {
    case 'TCon': return type.kind;
    case 'TVar': return env.get(type.name) || type.kind;
    case 'TApp': {
      const conKind = inferKind(type.con, env);
      const argKind = inferKind(type.arg, env);
      if (conKind.tag !== 'KArrow') {
        throw new Error(`Kind error: ${type.con} has kind ${conKind}, expected arrow kind`);
      }
      if (!kindEquals(conKind.from, argKind)) {
        throw new Error(`Kind mismatch: ${type.con} expects ${conKind.from}, got ${argKind}`);
      }
      return conKind.to;
    }
    case 'TArrow': {
      const fromKind = inferKind(type.from, env);
      const toKind = inferKind(type.to, env);
      if (!kindEquals(fromKind, kStar) || !kindEquals(toKind, kStar)) {
        throw new Error(`Arrow types must have kind ★`);
      }
      return kStar;
    }
    default:
      throw new Error(`Cannot infer kind of ${type.tag}`);
  }
}

// ============================================================
// Typeclasses
// ============================================================

class Typeclass {
  constructor(name, param, paramKind, methods) {
    this.name = name;
    this.param = param;     // type variable name (e.g., 'f')
    this.paramKind = paramKind;
    this.methods = methods; // Map<name, type scheme>
  }
}

class Instance {
  constructor(typeclass, type, implementations) {
    this.typeclass = typeclass;
    this.type = type;          // concrete type constructor
    this.implementations = implementations; // Map<methodName, implementation>
  }
}

// ============================================================
// Functor
// ============================================================

const functorClass = new Typeclass('Functor', 'f', kStarToStar, new Map([
  // fmap :: (a → b) → f a → f b
  ['fmap', { 
    description: '(a → b) → f a → f b',
    check: (impl, type) => typeof impl === 'function'
  }],
]));

function fmap(instance, fn, fa) {
  return instance.implementations.get('fmap')(fn, fa);
}

// List Functor
const listFunctor = new Instance(functorClass, tList, new Map([
  ['fmap', (fn, list) => list.map(fn)],
]));

// Maybe Functor (Just/Nothing encoded as {tag, value})
const maybeFunctor = new Instance(functorClass, tMaybe, new Map([
  ['fmap', (fn, maybe) => maybe.tag === 'Just' ? { tag: 'Just', value: fn(maybe.value) } : maybe],
]));

// Either Functor (maps over Right)
const eitherFunctor = new Instance(functorClass, tEither, new Map([
  ['fmap', (fn, either) => either.tag === 'Right' ? { tag: 'Right', value: fn(either.value) } : either],
]));

// IO Functor
const ioFunctor = new Instance(functorClass, tIO, new Map([
  ['fmap', (fn, io) => () => fn(io())],
]));

// ============================================================
// Monad
// ============================================================

const monadClass = new Typeclass('Monad', 'f', kStarToStar, new Map([
  // return :: a → f a
  ['return', { description: 'a → f a' }],
  // bind :: f a → (a → f b) → f b
  ['bind', { description: 'f a → (a → f b) → f b' }],
]));

function mreturn(instance, value) {
  return instance.implementations.get('return')(value);
}

function mbind(instance, ma, fn) {
  return instance.implementations.get('bind')(ma, fn);
}

// List Monad
const listMonad = new Instance(monadClass, tList, new Map([
  ['return', x => [x]],
  ['bind', (list, fn) => list.flatMap(fn)],
]));

// Maybe Monad
const maybeMonad = new Instance(monadClass, tMaybe, new Map([
  ['return', x => ({ tag: 'Just', value: x })],
  ['bind', (maybe, fn) => maybe.tag === 'Just' ? fn(maybe.value) : maybe],
]));

// IO Monad
const ioMonad = new Instance(monadClass, tIO, new Map([
  ['return', x => () => x],
  ['bind', (io, fn) => () => fn(io())()],
]));

// ============================================================
// Registry
// ============================================================

const INSTANCES = new Map([
  ['Functor:List', listFunctor],
  ['Functor:Maybe', maybeFunctor],
  ['Functor:Either', eitherFunctor],
  ['Functor:IO', ioFunctor],
  ['Monad:List', listMonad],
  ['Monad:Maybe', maybeMonad],
  ['Monad:IO', ioMonad],
]);

function getInstance(className, typeName) {
  return INSTANCES.get(`${className}:${typeName}`);
}

// ============================================================
// Monad Laws (for testing)
// ============================================================

function checkLeftIdentity(monad, value, fn) {
  // return a >>= f  ≡  f a
  const lhs = mbind(monad, mreturn(monad, value), fn);
  const rhs = fn(value);
  return JSON.stringify(lhs) === JSON.stringify(rhs);
}

function checkRightIdentity(monad, ma) {
  // m >>= return  ≡  m
  const lhs = mbind(monad, ma, x => mreturn(monad, x));
  return JSON.stringify(lhs) === JSON.stringify(ma);
}

function checkAssociativity(monad, ma, f, g) {
  // (m >>= f) >>= g  ≡  m >>= (λx. f x >>= g)
  const lhs = mbind(monad, mbind(monad, ma, f), g);
  const rhs = mbind(monad, ma, x => mbind(monad, f(x), g));
  return JSON.stringify(lhs) === JSON.stringify(rhs);
}

// ============================================================
// Exports
// ============================================================

export {
  KStar, KArrow, kStar, kStarToStar, kStarStarToStar, kindEquals,
  TCon, TApp, TVar, TArrow,
  tInt, tBool, tStr, tList, tMaybe, tIO, tEither, tPair,
  listOf, maybeOf, ioOf, eitherOf, pairOf,
  inferKind,
  Typeclass, Instance,
  functorClass, monadClass,
  listFunctor, maybeFunctor, eitherFunctor, ioFunctor,
  listMonad, maybeMonad, ioMonad,
  fmap, mreturn, mbind,
  getInstance, INSTANCES,
  checkLeftIdentity, checkRightIdentity, checkAssociativity
};
