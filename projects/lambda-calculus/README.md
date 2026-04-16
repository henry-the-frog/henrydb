# Lambda Calculus PL Theory Library

A comprehensive programming language theory library implemented in JavaScript.
**30 modules** covering the full spectrum from foundations to research-level topics.

## Module Catalog

### Foundations (1-10)
| # | Module | File | Tests | Description |
|---|--------|------|-------|-------------|
| 1 | Untyped λ-calculus | `lambda.js` | 21 | Church encoding, reduction strategies |
| 2 | De Bruijn indices | `debruijn-reduce.js` | 7 | Nameless representation |
| 3 | SKI combinators | `ski.js` | 11 | Compile λ to combinators |
| 4 | CPS transform | `cps.js` | 10 | Continuation-passing style |
| 5 | NbE | `nbe.js` | 6 | Normalization by evaluation |
| 6 | Computability | `computability.test.js` | 13 | Turing completeness proofs |
| 7 | STLC | `stlc.js` | 20 | Simply typed λ-calculus |
| 8 | System F | `systemf.js` | 13 | Polymorphic types (∀α. T) |
| 9 | CoC | `coc.js` | 43 | Calculus of Constructions (Π types, ★:□) |
| 10 | Curry-Howard | `curry-howard.js` | 8 | Propositions as types |

### Advanced Type Systems (11-20)
| # | Module | File | Tests | Description |
|---|--------|------|-------|-------------|
| 11 | Inductive types | `inductive.js` | 24 | Bool, Maybe, List, Either, Pair |
| 12 | Theorem proving | `theorems.test.js` | 28 | De Morgan, associativity, functors |
| 13 | Proof assistant | `proof-assistant.js` | 30 | Tactic language (intro, apply, refl) |
| 14 | Algebraic effects | `effects.js` | 28 | Perform/Handle, effect handlers |
| 15 | Effect types | `effects-types.js` | 35 | Effect rows, handler typing |
| 16 | Linear types | `linear.js` | 26 | Linear/affine/relevant modalities |
| 17 | Session types | `session-types.js` | 26 | Typed communication protocols |
| 18 | Gradual types | `gradual.js` | 46 | Dynamic type (?), consistency, blame |
| 19 | Refinement types | `refinement.js` | 33 | Predicate-refined types ({x:T | P}) |
| 20 | Row polymorphism | `row-poly.js` | 22 | Extensible records, structural subtyping |

### Research Level (21-30)
| # | Module | File | Tests | Description |
|---|--------|------|-------|-------------|
| 21 | Delimited continuations | `delimited.js` | 32 | shift/reset, multi-shot continuations |
| 22 | Intersection & union | `intersection-union.js` | 32 | TypeScript-style & and \| operators |
| 23 | Higher-kinded types | `hkt.js` | 28 | Kind system, Functor/Monad typeclasses |
| 24 | Type-level computation | `type-level.js` | 48 | Peano arithmetic at the type level |
| 25 | Small-step semantics | `small-step.js` | 14 | Reduction strategy tracer |
| 26 | Effects Rosetta | `effects-rosetta.js` | 23 | Same programs: monads vs effects vs continuations |
| 27 | CEK machine | `cek.js` | 22 | Abstract machine with explicit continuations |
| 28 | Abstract interpretation | `abstract-interp.js` | 24 | Sign, Interval, Constant domains |
| 29 | Lambda properties | `properties.test.js` | 8 | Church-Rosser, normalization properties |
| 30 | Hindley-Milner | `hindley-milner.js` | 21 | Algorithm W, let-polymorphism |

## Grand Total: 721+ tests, 0 failures

## Key Properties Implemented

- **Lambda cube**: Untyped → STLC → System F → CoC
- **Substructural**: Linear, Affine, Relevant, Unrestricted
- **Effects**: Algebraic effects, Delimited continuations, Effect typing, Monads
- **Proofs**: Dependent types, Induction, 28+ verified theorems
- **Practical**: Gradual types, Row polymorphism, Session types, Refinement types
- **Machines**: CEK machine, NbE, CPS transform
- **Analysis**: Abstract interpretation, Hindley-Milner inference
- **Teaching**: Effects Rosetta Stone, Small-step tracer, Type-level computation

## Interactive Tools

```bash
# Proof assistant REPL
node proof-repl.js

# Run all tests
for f in *.test.js; do node "$f"; done

# Run specific module
node cek.test.js
node hindley-milner.test.js
```

## Project Structure

```
lambda-calculus/
├── lambda.js              # Core untyped λ-calculus
├── stlc.js → systemf.js → coc.js  # Type system progression
├── effects.js → effects-types.js   # Effect system
├── delimited.js → effects-rosetta.js  # Continuation/effect equivalence
├── hindley-milner.js      # Type inference
├── proof-assistant.js     # Interactive proving
├── cek.js                 # Abstract machine
├── abstract-interp.js     # Static analysis
└── *.test.js              # Tests for each module
```
