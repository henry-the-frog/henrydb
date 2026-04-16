/**
 * 🎉🎉🎉 MODULE #160: Arrows — Generalized function composition 🎉🎉🎉
 * 
 * Arrows generalize functions to computations with:
 * - arr :: (a → b) → p a b          (lift pure function)
 * - >>> :: p a b → p b c → p a c    (sequential composition)
 * - first :: p a b → p (a,c) (b,c)  (run on first component)
 * - *** :: p a b → p c d → p (a,c) (b,d)  (parallel)
 * - &&& :: p a b → p a c → p a (b,c)      (fan-out)
 */

class SimpleArrow {
  constructor(fn) { this.fn = fn; }
  
  static arr(f) { return new SimpleArrow(f); }
  
  compose(other) { return new SimpleArrow(x => other.fn(this.fn(x))); }
  
  first() { return new SimpleArrow(([a, c]) => [this.fn(a), c]); }
  second() { return new SimpleArrow(([c, a]) => [c, this.fn(a)]); }
  
  parallel(other) { return new SimpleArrow(([a, c]) => [this.fn(a), other.fn(c)]); }
  fanout(other) { return new SimpleArrow(a => [this.fn(a), other.fn(a)]); }
  
  run(input) { return this.fn(input); }
}

// Kleisli arrow: functions that return Maybe
class KleisliArrow {
  constructor(fn) { this.fn = fn; } // a → Maybe<b>
  
  static arr(f) { return new KleisliArrow(x => ({ value: f(x), ok: true })); }
  
  compose(other) {
    return new KleisliArrow(x => {
      const r = this.fn(x);
      if (!r.ok) return r;
      return other.fn(r.value);
    });
  }
  
  first() {
    return new KleisliArrow(([a, c]) => {
      const r = this.fn(a);
      return r.ok ? { value: [r.value, c], ok: true } : { ok: false };
    });
  }
  
  run(input) { return this.fn(input); }
}

// Arrow combinators
function loop(arrow, init) {
  // ArrowLoop: tie the knot (feedback)
  return new SimpleArrow(a => {
    let state = init;
    const [result, newState] = arrow.fn([a, state]);
    return result;
  });
}

// Circuit: stateful arrow
function accumulator(init) {
  let state = init;
  return new SimpleArrow(input => {
    state += input;
    return state;
  });
}

export { SimpleArrow, KleisliArrow, loop, accumulator };
