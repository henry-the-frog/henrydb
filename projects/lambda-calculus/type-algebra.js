/**
 * Type Algebra: Types as Algebraic Structures
 * 
 * Types form a semiring:
 * - + is sum type (Either): 0 = Void, commutativity, associativity
 * - × is product type (Pair): 1 = Unit, commutativity, associativity
 * - Distribution: a × (b + c) = (a × b) + (a × c)
 * - → is exponential: a → b ≈ b^a
 * 
 * Count inhabitants to verify algebraically!
 */

// Type algebra values
function Void() { return 0; }          // 0 inhabitants
function Unit() { return 1; }          // 1 inhabitant
function Bool() { return 2; }          // 2 inhabitants
function Byte() { return 256; }        // 256 inhabitants

function Sum(a, b) { return a + b; }           // |A + B| = |A| + |B|
function Prod(a, b) { return a * b; }          // |A × B| = |A| × |B|
function Exp(a, b) { return Math.pow(b, a); }  // |A → B| = |B|^|A|

// Algebraic identities
function verifyIdentity(name, left, right) {
  return { name, left, right, holds: left === right };
}

function algebraicIdentities() {
  const v = Void(), u = Unit(), b = Bool(), by = Byte();
  return [
    // Additive identity: A + 0 = A
    verifyIdentity('A + 0 = A', Sum(b, v), b),
    // Multiplicative identity: A × 1 = A
    verifyIdentity('A × 1 = A', Prod(b, u), b),
    // Absorbing: A × 0 = 0
    verifyIdentity('A × 0 = 0', Prod(b, v), v),
    // Commutativity of +
    verifyIdentity('A + B = B + A', Sum(b, by), Sum(by, b)),
    // Commutativity of ×
    verifyIdentity('A × B = B × A', Prod(b, by), Prod(by, b)),
    // Associativity of +
    verifyIdentity('(A+B)+C = A+(B+C)', Sum(Sum(2, 3), 4), Sum(2, Sum(3, 4))),
    // Distribution
    verifyIdentity('A×(B+C) = A×B + A×C', Prod(2, Sum(3, 4)), Sum(Prod(2, 3), Prod(2, 4))),
    // Exponential: (A→B)→C = C^(B^A)
    verifyIdentity('(A→B)→C', Exp(Exp(2, 3), 4), Math.pow(4, Math.pow(3, 2))),
    // Curry: A→(B→C) = (A×B)→C, i.e., C^B^A = C^(A×B)
    verifyIdentity('A→(B→C) = (A×B)→C', Exp(2, Exp(3, 4)), Exp(Prod(2, 3), 4)),
    // Bool = 1 + 1
    verifyIdentity('Bool = 1 + 1', Bool(), Sum(Unit(), Unit())),
    // Maybe A = 1 + A
    verifyIdentity('Maybe Bool = 1 + 2 = 3', Sum(u, b), 3),
    // List has infinitely many inhabitants (skip count, just verify structure)
  ];
}

// Derivative of a type (Conor McBride's "one-hole contexts")
// d/da(1) = 0, d/da(a) = 1, d/da(a×b) = da×b + a×db, d/da(a+b) = da + db
function derivative(type) {
  switch (type.tag) {
    case 'TConst': return { tag: 'TConst', value: 0 }; // d/da(const) = 0
    case 'TVar': return { tag: 'TConst', value: 1 };   // d/da(a) = 1
    case 'TSum': return { tag: 'TSum', left: derivative(type.left), right: derivative(type.right) };
    case 'TProd': return {
      tag: 'TSum',
      left: { tag: 'TProd', left: derivative(type.left), right: type.right },
      right: { tag: 'TProd', left: type.left, right: derivative(type.right) }
    };
    default: return { tag: 'TConst', value: 0 };
  }
}

function evalTypeExpr(type) {
  switch (type.tag) {
    case 'TConst': return type.value;
    case 'TVar': return type.value;
    case 'TSum': return evalTypeExpr(type.left) + evalTypeExpr(type.right);
    case 'TProd': return evalTypeExpr(type.left) * evalTypeExpr(type.right);
    default: return 0;
  }
}

export { Void, Unit, Bool, Byte, Sum, Prod, Exp, verifyIdentity, algebraicIdentities, derivative, evalTypeExpr };
