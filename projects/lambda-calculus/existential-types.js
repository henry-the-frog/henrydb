/**
 * Existential Types
 * 
 * ∃a. {type: a, operations: ...} — hide the representation type
 * 
 * The consumer knows ONLY the operations, not the internal type.
 * This is how abstract data types (ADTs) work in ML modules.
 * 
 * Example: ∃a. {empty: a, push: Int → a → a, pop: a → (Int, a)}
 * Could be implemented with arrays, linked lists, etc.
 * Consumer can't tell — that's the point.
 */

class ExistentialPackage {
  constructor(witnessType, value, operations) {
    this.witnessType = witnessType;  // The hidden type name
    this.value = value;              // The actual value
    this.operations = operations;     // Map<name, function>
  }

  /**
   * Use the package: can only access via operations, not the value directly
   */
  open(body) {
    return body(this.value, this.operations);
  }
}

/**
 * Pack: create an existential by hiding the implementation type
 * pack [τ, {val, ops}] as ∃α.T
 */
function pack(type, value, operations) {
  return new ExistentialPackage(type, value, operations);
}

/**
 * Unpack: use an existential (the type variable is abstract)
 * unpack [α, x] = package in body
 */
function unpack(pkg, body) {
  return pkg.open(body);
}

// ============================================================
// Example: Abstract Counter
// ============================================================

// Implementation 1: Integer counter
const intCounter = pack('Int', 0, {
  zero: () => 0,
  increment: (c) => c + 1,
  decrement: (c) => c - 1,
  get: (c) => c,
});

// Implementation 2: Object counter (same interface!)
const objCounter = pack('Object', { count: 0 }, {
  zero: () => ({ count: 0 }),
  increment: (c) => ({ count: c.count + 1 }),
  decrement: (c) => ({ count: c.count - 1 }),
  get: (c) => c.count,
});

// Implementation 3: Array-length counter (absurd but valid)
const arrCounter = pack('Array', [], {
  zero: () => [],
  increment: (c) => [...c, null],
  decrement: (c) => c.slice(0, -1),
  get: (c) => c.length,
});

// ============================================================
// Example: Abstract Stack
// ============================================================

const listStack = pack('List', [], {
  empty: () => [],
  push: (x, s) => [x, ...s],
  pop: (s) => s.length > 0 ? { value: s[0], rest: s.slice(1) } : null,
  isEmpty: (s) => s.length === 0,
});

// ============================================================
// Type checking: ensure abstraction is respected
// ============================================================

function checkAbstraction(pkg1, pkg2, testFn) {
  // Both packages should behave identically through their operations
  const result1 = testFn(pkg1);
  const result2 = testFn(pkg2);
  return result1 === result2;
}

export { ExistentialPackage, pack, unpack, intCounter, objCounter, arrCounter, listStack, checkAbstraction };
