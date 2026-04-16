/**
 * Levity Polymorphism: Polymorphism over runtime representation
 * 
 * GHC-style: functions can be polymorphic over whether their
 * argument is lifted (boxed, lazy) or unlifted (unboxed, strict).
 * 
 * Kind: TYPE :: RuntimeRep → Type
 * Lifted: TYPE 'LiftedRep (normal Haskell values, can be ⊥)
 * Unlifted: TYPE 'IntRep, TYPE 'DoubleRep (machine values, never ⊥)
 */

const RuntimeRep = {
  LiftedRep: 'LiftedRep',     // Boxed, lazy, can be ⊥
  IntRep: 'IntRep',           // Machine Int#
  DoubleRep: 'DoubleRep',     // Machine Double#
  TupleRep: 'TupleRep',       // Unboxed tuple
  SumRep: 'SumRep',           // Unboxed sum
};

class LevityType {
  constructor(name, rep) { this.name = name; this.rep = rep; }
  isLifted() { return this.rep === RuntimeRep.LiftedRep; }
  isUnlifted() { return this.rep !== RuntimeRep.LiftedRep; }
  toString() { return `${this.name} :: TYPE '${this.rep}`; }
}

// Standard types
const tInt = new LevityType('Int', RuntimeRep.LiftedRep);
const tIntHash = new LevityType('Int#', RuntimeRep.IntRep);
const tDouble = new LevityType('Double', RuntimeRep.LiftedRep);
const tDoubleHash = new LevityType('Double#', RuntimeRep.DoubleRep);
const tBool = new LevityType('Bool', RuntimeRep.LiftedRep);

// Can this type appear in a polymorphic position?
function canBeLevityPolymorphic(type) {
  // Levity-polymorphic arguments are restricted: can't be passed to regular functions
  return type.isLifted();
}

// Check if function can accept this argument type
function checkLevity(paramRep, argType) {
  if (paramRep === 'any') return true; // Levity-polymorphic parameter
  return argType.rep === paramRep;
}

// Boxing/unboxing
function box(value, rep) {
  return { boxed: true, value, originalRep: rep };
}

function unbox(boxed) {
  return { boxed: false, value: boxed.value, rep: boxed.originalRep };
}

// Determine calling convention
function callingConvention(argTypes) {
  const hasUnlifted = argTypes.some(t => t.isUnlifted());
  return hasUnlifted ? 'worker' : 'wrapper';
}

export { RuntimeRep, LevityType, tInt, tIntHash, tDouble, tDoubleHash, tBool, canBeLevityPolymorphic, checkLevity, box, unbox, callingConvention };
