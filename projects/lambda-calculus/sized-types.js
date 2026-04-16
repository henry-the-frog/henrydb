/**
 * Sized Types: Size annotations for guaranteed termination
 * 
 * Add a size index to recursive types to prove termination:
 * Nat^i is a natural number of size at most i.
 * A function f : Nat^i → Nat^(i+1) is guaranteed to terminate
 * because i strictly decreases in recursive calls.
 */

const INFTY = Infinity;

class SizedType {
  constructor(base, size) { this.base = base; this.size = size; }
  toString() { return this.size === INFTY ? this.base : `${this.base}^${this.size}`; }
}

function sNat(size) { return new SizedType('Nat', size); }
function sList(size) { return new SizedType('List', size); }
function sTree(size) { return new SizedType('Tree', size); }

// Size ordering
function sizeLeq(s1, s2) {
  if (s2 === INFTY) return true;
  if (s1 === INFTY) return false;
  return s1 <= s2;
}

function sizeSucc(s) { return s === INFTY ? INFTY : s + 1; }
function sizePred(s) { return s === INFTY ? INFTY : Math.max(0, s - 1); }

// Check termination: recursive calls must decrease size
function checkSizedTermination(fnName, calls) {
  for (const call of calls) {
    if (call.fn !== fnName) continue;
    if (!call.argSizes.every((argSize, i) => {
      const paramSize = call.paramSizes[i];
      return sizeLeq(argSize, sizePred(paramSize));
    })) {
      return { terminates: false, reason: `Call doesn't decrease size: ${JSON.stringify(call)}` };
    }
  }
  return { terminates: true };
}

// Subtyping: Nat^i <: Nat^j when i ≤ j
function isSubtype(t1, t2) {
  return t1.base === t2.base && sizeLeq(t1.size, t2.size);
}

// Example: factorial is size-preserving
// fact : Nat^i → Nat^∞
// fact 0 = 1
// fact (n+1) = (n+1) * fact n   (n has size i-1 < i ✓)

export { SizedType, sNat, sList, sTree, sizeLeq, sizeSucc, sizePred, checkSizedTermination, isSubtype, INFTY };
