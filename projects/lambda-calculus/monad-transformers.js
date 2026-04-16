/**
 * Monad Transformers: Stack effects compositionally
 */

class Identity { constructor(v) { this.value = v; } static of(v) { return new Identity(v); } chain(f) { return f(this.value); } map(f) { return new Identity(f(this.value)); } }

// MaybeT
class MaybeT {
  constructor(run) { this.run = run; } // m (Maybe a)
  static of(M) { return v => new MaybeT(M.of({ tag: 'Just', value: v })); }
  static nothing(M) { return new MaybeT(M.of({ tag: 'Nothing' })); }
  static lift(ma) { return new MaybeT(ma.map(v => ({ tag: 'Just', value: v }))); }
  chain(M, f) {
    return new MaybeT(this.run.chain(maybe =>
      maybe.tag === 'Nothing' ? M.of({ tag: 'Nothing' }) : f(maybe.value).run
    ));
  }
  map(M, f) {
    return new MaybeT(this.run.map(maybe =>
      maybe.tag === 'Nothing' ? maybe : { tag: 'Just', value: f(maybe.value) }
    ));
  }
}

// StateT
class StateT {
  constructor(run) { this.run = run; } // s → m (a, s)
  static of(M) { return v => new StateT(s => M.of([v, s])); }
  static get(M) { return new StateT(s => M.of([s, s])); }
  static put(M, s) { return new StateT(_ => M.of([null, s])); }
  static lift(M, ma) { return new StateT(s => ma.map(v => [v, s])); }
  chain(M, f) {
    return new StateT(s => this.run(s).chain(([a, s2]) => f(a).run(s2)));
  }
  map(M, f) {
    return new StateT(s => this.run(s).map(([a, s2]) => [f(a), s2]));
  }
  exec(s) { return this.run(s); }
}

// ReaderT
class ReaderT {
  constructor(run) { this.run = run; } // r → m a
  static of(M) { return v => new ReaderT(_ => M.of(v)); }
  static ask(M) { return new ReaderT(r => M.of(r)); }
  static lift(ma) { return new ReaderT(_ => ma); }
  chain(M, f) { return new ReaderT(r => this.run(r).chain(a => f(a).run(r))); }
  map(M, f) { return new ReaderT(r => this.run(r).map(f)); }
}

export { Identity, MaybeT, StateT, ReaderT };
