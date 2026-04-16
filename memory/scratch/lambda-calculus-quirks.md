# Lambda Calculus: Church Encoding Quirks

**Created:** 2026-04-15
**Uses:** 1

## Church Exponentiation Zero-Exponent Bug

Church exp = `λm n. n m`. When n=0 (the exponent):
- `exp(k, 0) = 0 k = (λf x.x) k = λx.x`
- This gives the identity function (arity 1), NOT Church 1 (`λf x.f x`, arity 2)
- Mathematically k^0 = 1, but Church encoding doesn't preserve this
- Known limitation of the standard Church exponentiation encoding

## Capture-Avoiding Substitution

Fresh variable generation with a counter works but names grow. Alternative: use de Bruijn indices for all internal operations and only convert to named for display.

## Reduction Strategy Key Insight

Normal-order is the ONLY strategy guaranteed to find a normal form if one exists (Church-Rosser theorem). This is why Haskell uses lazy evaluation (call-by-need ≈ call-by-name with sharing).

Example: `K a Ω` where Ω = `(λx.x x)(λx.x x)`
- Normal/CBN: reduces to `a` (never evaluates Ω)
- Applicative/CBV: diverges (tries to evaluate Ω first)

## Scott vs Church Encodings

- **Church numerals**: fold/catamorphism encoding. Good for iteration but predecessor is O(n).
- **Scott numerals**: case analysis encoding. Predecessor is O(1) but no built-in iteration.
- Scott encoding is more natural for pattern matching (like ML/Haskell case expressions).
