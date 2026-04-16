/**
 * Constraint Kinds: Kinds that are constraints (GHC-style)
 * type Showable a = Show a => a
 */

class Kind { constructor(name) { this.name = name; } toString() { return this.name; } }
const Star = new Kind('*');
const Constraint = new Kind('Constraint');
const kFun = (a, b) => new Kind(`${a} → ${b}`);

class ConstraintKind {
  constructor(className, typeParam) { this.className = className; this.typeParam = typeParam; this.kind = Constraint; }
  toString() { return `${this.className} ${this.typeParam}`; }
}

class ConstrainedType {
  constructor(constraints, type) { this.constraints = constraints; this.type = type; }
  toString() { return `(${this.constraints.join(', ')}) => ${this.type}`; }
}

function hasConstraint(ct, className) { return ct.constraints.some(c => c.className === className); }
function addConstraint(ct, constraint) { return new ConstrainedType([...ct.constraints, constraint], ct.type); }
function removeConstraint(ct, className) { return new ConstrainedType(ct.constraints.filter(c => c.className !== className), ct.type); }
function satisfies(ct, available) { return ct.constraints.every(c => available.has(c.className)); }

export { Kind, Star, Constraint, kFun, ConstraintKind, ConstrainedType, hasConstraint, addConstraint, removeConstraint, satisfies };
