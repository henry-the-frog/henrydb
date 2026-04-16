/**
 * Applicative Functors: Between Functor and Monad
 * 
 * pure :: a → f a
 * <*> :: f (a → b) → f a → f b
 * 
 * Can combine independent effects but not dependent ones.
 * Allows static analysis of effects (unlike monads).
 */

class Identity {
  constructor(value) { this.value = value; }
  static pure(x) { return new Identity(x); }
  map(f) { return new Identity(f(this.value)); }
  ap(other) { return new Identity(this.value(other.value)); }
}

class Maybe {
  constructor(value) { this.value = value; this.isNothing = value === null; }
  static pure(x) { return new Maybe(x); }
  static nothing() { return new Maybe(null); }
  map(f) { return this.isNothing ? this : new Maybe(f(this.value)); }
  ap(other) { return this.isNothing || other.isNothing ? Maybe.nothing() : new Maybe(this.value(other.value)); }
}

class Validation {
  constructor(value, errors) { this.value = value; this.errors = errors || []; this.isValid = errors === null || errors.length === 0; }
  static success(x) { return new Validation(x, null); }
  static failure(errs) { return new Validation(null, errs); }
  map(f) { return this.isValid ? Validation.success(f(this.value)) : this; }
  ap(other) {
    if (this.isValid && other.isValid) return Validation.success(this.value(other.value));
    if (!this.isValid && !other.isValid) return Validation.failure([...this.errors, ...other.errors]);
    return this.isValid ? other : this;
  }
}

// Applicative combinators
function liftA2(f, fa, fb) { return fa.map(a => b => f(a, b)).ap(fb); }
function sequenceA(list, Pure) { return list.reduce((acc, fa) => liftA2((xs, x) => [...xs, x], acc, fa), Pure.pure([])); }
function traverse(list, f, Pure) { return sequenceA(list.map(f), Pure); }

// Laws
function checkIdentityLaw(Pure, v) { return Pure.pure(x => x).ap(v).value === v.value; }
function checkHomomorphismLaw(Pure, f, x) { return Pure.pure(f).ap(Pure.pure(x)).value === Pure.pure(f(x)).value; }

export { Identity, Maybe, Validation, liftA2, sequenceA, traverse, checkIdentityLaw, checkHomomorphismLaw };
