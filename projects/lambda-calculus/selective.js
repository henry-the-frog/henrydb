/**
 * Selective Functors: Between Applicative and Monad
 * 
 * Selective functors allow you to select which effects to run
 * based on results of previous effects, but with less power than monads.
 * 
 * select :: f (Either a b) → f (a → b) → f b
 */

class Selective {
  constructor(value) { this._value = value; }
  
  static pure(x) { return new Selective({ tag: 'Right', value: x }); }
  static left(x) { return new Selective({ tag: 'Left', value: x }); }
  static right(x) { return new Selective({ tag: 'Right', value: x }); }
  
  map(f) {
    if (this._value.tag === 'Right') return new Selective({ tag: 'Right', value: f(this._value.value) });
    return this;
  }
  
  get value() { return this._value; }
}

function select(feither, fhandler) {
  if (feither.value.tag === 'Right') return feither; // Skip handler
  return new Selective({ tag: 'Right', value: fhandler.value.value(feither.value.value) });
}

// ifS :: f Bool → f a → f a → f a (selective if)
function ifS(cond, thenBranch, elseBranch) {
  const mapped = cond.map(b => b ? { tag: 'Right', value: null } : { tag: 'Left', value: null });
  // If true: use then, if false: use else
  if (cond.value.tag === 'Right' && cond.value.value) return thenBranch;
  return elseBranch;
}

// whenS :: f Bool → f () → f ()
function whenS(cond, action) {
  if (cond.value.tag === 'Right' && cond.value.value) return action;
  return Selective.pure(null);
}

// Branch: two-way select
function branch(feither, fLeft, fRight) {
  if (feither.value.tag === 'Left') {
    return new Selective({ tag: 'Right', value: fLeft.value.value(feither.value.value) });
  }
  return new Selective({ tag: 'Right', value: fRight.value.value(feither.value.value) });
}

// Static analysis: can we determine which branch without running?
function isStaticRight(sel) { return sel.value.tag === 'Right'; }
function isStaticLeft(sel) { return sel.value.tag === 'Left'; }

export { Selective, select, ifS, whenS, branch, isStaticRight, isStaticLeft };
