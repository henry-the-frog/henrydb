/**
 * Quotient Types
 * 
 * A quotient type identifies elements that are "equivalent" under some relation.
 * Q = A / ~  where ~ is an equivalence relation on A
 * 
 * Examples:
 * - Int = (Nat × Nat) / ~ where (a,b) ~ (c,d) iff a+d = b+c
 * - Rational = (Int × Int) / ~ where (a,b) ~ (c,d) iff a*d = b*c
 * - Set = List / ~ where xs ~ ys iff they have same elements
 * - Unordered pair = (A × A) / ~ where (a,b) ~ (b,a)
 */

class Quotient {
  constructor(name, repr, equiv, normalize = null) {
    this.name = name;
    this.repr = repr;       // Representation type description
    this.equiv = equiv;     // (a, b) → bool: equivalence relation
    this.normalize = normalize; // Optional normalizer for canonical forms
  }

  /**
   * Create a quotient element (optionally normalize)
   */
  mk(value) {
    const normalized = this.normalize ? this.normalize(value) : value;
    return { value: normalized, quotient: this.name };
  }

  /**
   * Check if two elements are equivalent
   */
  eq(a, b) {
    return this.equiv(a.value, b.value);
  }

  /**
   * Apply a function that respects the quotient (well-defined on equivalence classes)
   */
  lift(f, a) {
    return f(a.value);
  }

  /**
   * Verify the equivalence relation is valid
   */
  verifyEquivalence(samples) {
    // Reflexive
    for (const x of samples) {
      if (!this.equiv(x, x)) return { valid: false, error: `Not reflexive: ${x}` };
    }
    // Symmetric
    for (let i = 0; i < samples.length; i++) {
      for (let j = i + 1; j < samples.length; j++) {
        if (this.equiv(samples[i], samples[j]) !== this.equiv(samples[j], samples[i])) {
          return { valid: false, error: `Not symmetric: ${samples[i]}, ${samples[j]}` };
        }
      }
    }
    return { valid: true };
  }
}

// ============================================================
// Examples
// ============================================================

// Rational numbers: (Int × Int) / ~ where (a,b) ~ (c,d) iff a*d = b*c
const Rational = new Quotient('Rational', '(Int × Int)',
  ([a, b], [c, d]) => a * d === b * c,
  ([n, d]) => {
    if (d === 0) throw new Error('Division by zero');
    const g = gcd(Math.abs(n), Math.abs(d));
    const sign = d < 0 ? -1 : 1;
    return [sign * n / g, sign * d / g];
  }
);

function gcd(a, b) { return b === 0 ? a : gcd(b, a % b); }

// Modular arithmetic: Int / n
function ModN(n) {
  return new Quotient(`Z/${n}Z`, 'Int',
    (a, b) => ((a % n) + n) % n === ((b % n) + n) % n,
    x => ((x % n) + n) % n
  );
}

// Unordered pairs: (A × A) / swap
const UnorderedPair = new Quotient('UnorderedPair', '(A × A)',
  ([a, b], [c, d]) => (a === c && b === d) || (a === d && b === c),
  ([a, b]) => a <= b ? [a, b] : [b, a]
);

// Sets (as lists): List / permutation
const SetQ = new Quotient('Set', 'List',
  (xs, ys) => xs.length === ys.length && xs.every(x => ys.includes(x)),
  xs => [...new Set(xs)].sort()
);

export { Quotient, Rational, ModN, UnorderedPair, SetQ, gcd };
