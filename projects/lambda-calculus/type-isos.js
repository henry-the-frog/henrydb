/**
 * Type Isomorphisms
 * 
 * Two types A and B are isomorphic (A ≅ B) if there exist functions
 *   f: A → B and g: B → A such that g∘f = id and f∘g = id
 * 
 * Classic isomorphisms:
 * - Curry/uncurry: A → (B → C) ≅ (A × B) → C
 * - Commutativity: A × B ≅ B × A, A + B ≅ B + A
 * - Associativity: (A × B) × C ≅ A × (B × C)
 * - Unit: A × 1 ≅ A, A + 0 ≅ A
 * - Distribution: A × (B + C) ≅ (A × B) + (A × C)
 * - Void: A → 0 ≅ 0 (if A inhabited), 0 → A ≅ 1
 * - Exponentials: A → B × C ≅ (A → B) × (A → C)
 */

// Isomorphism: a pair of functions that are inverses
class Iso {
  constructor(name, forward, backward) {
    this.name = name;
    this.forward = forward;   // A → B
    this.backward = backward; // B → A
  }

  verify(samples) {
    for (const a of samples) {
      const b = this.forward(a);
      const roundtrip = this.backward(b);
      if (JSON.stringify(a) !== JSON.stringify(roundtrip)) {
        return { ok: false, input: a, output: b, roundtrip, direction: 'forward→backward' };
      }
    }
    return { ok: true };
  }

  verifyBoth(samplesA, samplesB) {
    const fwd = this.verify(samplesA);
    if (!fwd.ok) return fwd;
    
    for (const b of samplesB) {
      const a = this.backward(b);
      const roundtrip = this.forward(a);
      if (JSON.stringify(b) !== JSON.stringify(roundtrip)) {
        return { ok: false, input: b, output: a, roundtrip, direction: 'backward→forward' };
      }
    }
    return { ok: true };
  }
}

// ============================================================
// Classic isomorphisms
// ============================================================

// Curry / Uncurry:  (A → B → C) ≅ ((A × B) → C)
const curryIso = new Iso('curry',
  f => ([a, b]) => f(a)(b),      // (A→B→C) → ((A×B)→C)
  g => a => b => g([a, b])       // ((A×B)→C) → (A→B→C)
);

// Product commutativity: A × B ≅ B × A
const prodCommute = new Iso('A×B ≅ B×A',
  ([a, b]) => [b, a],
  ([b, a]) => [a, b]
);

// Product associativity: (A × B) × C ≅ A × (B × C)
const prodAssoc = new Iso('(A×B)×C ≅ A×(B×C)',
  ([[a, b], c]) => [a, [b, c]],
  ([a, [b, c]]) => [[a, b], c]
);

// Sum commutativity: A + B ≅ B + A
const sumCommute = new Iso('A+B ≅ B+A',
  either => either.tag === 'Left' ? { tag: 'Right', value: either.value } : { tag: 'Left', value: either.value },
  either => either.tag === 'Left' ? { tag: 'Right', value: either.value } : { tag: 'Left', value: either.value }
);

// Unit: A × 1 ≅ A
const unitProd = new Iso('A×1 ≅ A',
  ([a, _]) => a,
  a => [a, null]
);

// Distribution: A × (B + C) ≅ (A × B) + (A × C)
const distribute = new Iso('A×(B+C) ≅ (A×B)+(A×C)',
  ([a, bc]) => bc.tag === 'Left'
    ? { tag: 'Left', value: [a, bc.value] }
    : { tag: 'Right', value: [a, bc.value] },
  abc => abc.tag === 'Left'
    ? [abc.value[0], { tag: 'Left', value: abc.value[1] }]
    : [abc.value[0], { tag: 'Right', value: abc.value[1] }]
);

// Exponential: A → B × C ≅ (A → B) × (A → C)
const expProd = new Iso('A→B×C ≅ (A→B)×(A→C)',
  f => [a => f(a)[0], a => f(a)[1]],
  ([g, h]) => a => [g(a), h(a)]
);

function Left(v) { return { tag: 'Left', value: v }; }
function Right(v) { return { tag: 'Right', value: v }; }

export {
  Iso, curryIso, prodCommute, prodAssoc, sumCommute, unitProd, distribute, expProd,
  Left, Right
};
