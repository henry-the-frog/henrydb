/**
 * Polarized Types: Positive and Negative
 * 
 * Positive types (data): defined by constructors (how to BUILD them)
 *   - Bool = True | False
 *   - Nat = Zero | Succ(Nat)
 *   - A × B, A + B
 * 
 * Negative types (codata): defined by destructors (how to USE them)
 *   - Stream = {head: A, tail: Stream A}
 *   - A → B (defined by: apply argument, get result)
 *   - A & B (lazy pair: project left or right)
 * 
 * Focusing: canonical forms for proof search. Values (positive) are intro'd,
 * computations (negative) are elim'd.
 */

// Polarity
const POS = 'positive';
const NEG = 'negative';

// Positive types
class TSum { constructor(name, cases) { this.tag = 'TSum'; this.polarity = POS; this.name = name; this.cases = cases; } }
class TProd { constructor(fst, snd) { this.tag = 'TProd'; this.polarity = POS; this.fst = fst; this.snd = snd; } }
class TUnit { constructor() { this.tag = 'TUnit'; this.polarity = POS; } }
class TVoid { constructor() { this.tag = 'TVoid'; this.polarity = POS; } }

// Negative types
class TFun { constructor(param, ret) { this.tag = 'TFun'; this.polarity = NEG; this.param = param; this.ret = ret; } }
class TWith { constructor(fst, snd) { this.tag = 'TWith'; this.polarity = NEG; this.fst = fst; this.snd = snd; } } // Lazy pair (negative product)
class TTop { constructor() { this.tag = 'TTop'; this.polarity = NEG; } }

// Shifts (mediate between polarities)
class TShift { constructor(inner) { this.tag = 'TShift'; this.inner = inner; } } // ↓: neg → pos (thunk)
class TForce { constructor(inner) { this.tag = 'TForce'; this.inner = inner; } } // ↑: pos → neg (return)

function polarity(type) {
  return type.polarity || (type.tag === 'TShift' ? POS : type.tag === 'TForce' ? NEG : null);
}

// ============================================================
// Values (positive: constructors)
// ============================================================

class VInj { constructor(tag, value) { this.tag = 'VInj'; this.which = tag; this.value = value; } } // Sum injection
class VPair { constructor(fst, snd) { this.tag = 'VPair'; this.fst = fst; this.snd = snd; } }
class VUnit { constructor() { this.tag = 'VUnit'; } }
class VThunk { constructor(comp) { this.tag = 'VThunk'; this.comp = comp; } } // ↓: thunk a negative

// Computations (negative: destructors)
class CLam { constructor(fn) { this.tag = 'CLam'; this.fn = fn; } }
class CWith { constructor(fst, snd) { this.tag = 'CWith'; this.fst = fst; this.snd = snd; } }
class CReturn { constructor(val) { this.tag = 'CReturn'; this.val = val; } } // ↑: return a positive

// ============================================================
// Focusing
// ============================================================

function isFocused(value) {
  // A focused value is a canonical form
  if (value.tag === 'VInj' || value.tag === 'VPair' || value.tag === 'VUnit' || value.tag === 'VThunk') return true;
  return false;
}

function isNeutral(comp) {
  // A neutral computation is waiting to be destructed
  if (comp.tag === 'CLam' || comp.tag === 'CWith') return true;
  return false;
}

// ============================================================
// Evaluate (focused evaluation)
// ============================================================

function apply(comp, arg) {
  if (comp.tag !== 'CLam') throw new Error('Apply: not a lambda');
  return comp.fn(arg);
}

function projectFst(comp) {
  if (comp.tag !== 'CWith') throw new Error('Project: not a with-pair');
  return comp.fst;
}

function projectSnd(comp) {
  if (comp.tag !== 'CWith') throw new Error('Project: not a with-pair');
  return comp.snd;
}

function matchSum(value, handlers) {
  if (value.tag !== 'VInj') throw new Error('Match: not an injection');
  const handler = handlers[value.which];
  if (!handler) throw new Error(`No handler for case: ${value.which}`);
  return handler(value.value);
}

function force(thunk) {
  if (thunk.tag !== 'VThunk') throw new Error('Force: not a thunk');
  return thunk.comp;
}

export {
  POS, NEG, TSum, TProd, TUnit, TVoid, TFun, TWith, TTop, TShift, TForce,
  polarity, VInj, VPair, VUnit, VThunk, CLam, CWith, CReturn,
  isFocused, isNeutral, apply, projectFst, projectSnd, matchSum, force
};
