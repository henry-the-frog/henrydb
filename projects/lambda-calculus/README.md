# Lambda Calculus — From Foundations to System F

A comprehensive exploration of lambda calculus and type theory, implemented from scratch in JavaScript. Zero dependencies.

## Modules

| Module | Description | Tests |
|--------|-------------|-------|
| **lambda.js** | Untyped lambda calculus: parser, 4 reduction strategies, de Bruijn indices, alpha-equivalence, Church encodings | 104 |
| **computability.test.js** | Fibonacci, SKI combinators, Scott encodings, boolean logic, divergence detection, stress tests | 55 |
| **ski.js** | SKI combinator calculus: abstraction elimination (Turner), B/C optimization, graph reduction, Unlambda notation | 49 |
| **stlc.js** | Simply Typed Lambda Calculus: bidirectional type checker, CBV evaluator, pairs, fix/recursion | 82 |
| **cps.js** | CPS transformation: Fischer/Plotkin, Danvy-Filinski one-pass, A-Normal Form (ANF) | 33 |
| **systemf.js** | System F (polymorphic λ-calculus): ∀-types, type abstraction/application, Church encodings, existentials, rank-2 | 39 |
| **curry-howard.js** | Curry-Howard correspondence: propositions as types, proofs as programs | 40 |
| **nbe.js** | Normalization by Evaluation: semantic domain, evaluate/readback, beta-eta equality | 29 |
| **properties.test.js** | Property-based tests: commutativity, associativity, distributivity, Church-Rosser, NbE≡reduction | 153 |
| **Total** | | **584** |

## Architecture

```
Untyped Lambda Calculus (lambda.js)
├── Parser (λx.body and \x.body syntax, multi-param)
├── 4 Reduction Strategies
│   ├── Normal-order (leftmost-outermost, always finds NF)
│   ├── Applicative-order (leftmost-innermost)
│   ├── Call-by-value (no reduction under λ)
│   └── Call-by-name (lazy, no arg evaluation)
├── De Bruijn Indices (nameless representation)
├── Alpha-Equivalence
├── Capture-Avoiding Substitution
└── Church Encodings (booleans, numerals, pairs, lists, Y/Z combinators)

SKI Combinator Calculus (ski.js)
├── Abstraction Elimination (λ-calculus → SKI)
│   ├── Basic (S, K, I only)
│   └── Optimized (+ B, C combinators — Turner's algorithm)
├── Graph Reduction
├── Unlambda Notation
└── Size Analysis

Simply Typed Lambda Calculus (stlc.js)
├── Types: Bool, Int, Unit, →, ×
├── Bidirectional Type Checking (infer + check)
├── CBV Evaluator
├── Fix-point Recursion
└── Strong Normalization (guaranteed without fix)

CPS Transformation (cps.js)
├── Fischer/Plotkin CPS
├── Danvy-Filinski One-Pass CPS (fewer admin redexes)
├── A-Normal Form (ANF)
└── Administrative Beta Reduction

System F — Polymorphic Lambda Calculus (systemf.js)
├── Type Variables, ∀-Quantification
├── Type Abstraction (Λα. t) and Type Application (t [T])
├── Type-Level Substitution (capture-avoiding)
├── Impredicative Polymorphism
├── Church Encodings (typed: CBool = ∀α.α→α→α, CNat = ∀α.(α→α)→α→α)
├── Existential Types (via encoding)
├── Higher-Rank Types (rank-2)
└── Parametricity / Free Theorems
```

## Key Insights Discovered

### Church Exponentiation Quirk
`exp(n, 0)` gives `λx.x` (the identity, arity 1) instead of Church 1 (`λf x.f x`, arity 2). This is because `0 m = (λf x.x) m = λx.x` for any m. A fundamental limitation of the standard Church encoding.

### Normal-Order vs Applicative-Order
Normal-order is the *only* strategy guaranteed to find a normal form when one exists (Church-Rosser theorem). Example: `K a Ω` reduces to `a` in normal-order but diverges in applicative-order.

### SKI Size Blowup
Turner's optimized abstraction elimination (using B and C combinators) produces terms that are consistently smaller than the basic S/K/I-only translation, but both are larger than the source lambda term.

### CPS One-Pass Advantage
Danvy-Filinski's one-pass CPS avoids creating administrative redexes by using meta-level continuations, producing more compact output than Fischer/Plotkin.

## Running Tests

```bash
node --test *.test.js      # All 362 tests
node --test lambda.test.js  # Core lambda calculus
node --test ski.test.js     # SKI combinators
node --test stlc.test.js    # Simply typed
node --test cps.test.js     # CPS transformation
node --test systemf.test.js # System F
```

## References

- Church, A. (1936) — "An Unsolvable Problem of Elementary Number Theory"
- Girard, J.-Y. (1972) — System F
- Turner, D. (1979) — SKI combinator compilation
- Danvy & Filinski (1992) — One-pass CPS
- Pierce, B. (2002) — *Types and Programming Languages*
