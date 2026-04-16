# Lambda Calculus PL Theory Library

A comprehensive programming language theory library implemented in JavaScript.
21 modules covering the full spectrum of type theory and programming language semantics.

## Module Catalog

### Foundations
| Module | File | Tests | Description |
|--------|------|-------|-------------|
| Untyped λ-calculus | `lambda.js` | 29 | Church encoding, reduction strategies |
| De Bruijn indices | `debruijn-reduce.js` | - | Nameless representation |
| SKI combinators | `ski.js` | 8 | Compile λ to combinators |
| CPS transform | `cps.js` | - | Continuation-passing style |
| NbE | `nbe.js` | 15 | Normalization by evaluation |
| Computability | `computability.test.js` | 10 | Turing completeness proofs |

### Type Systems
| Module | File | Tests | Description |
|--------|------|-------|-------------|
| STLC | `stlc.js` | 14 | Simply typed λ-calculus |
| System F | `systemf.js` | 15 | Polymorphic types (∀α. T) |
| CoC | `coc.js` | 43 | Calculus of Constructions (Π types, ★:□) |
| Gradual types | `gradual.js` | 46 | Dynamic type (?), consistency, blame |
| Linear types | `linear.js` | 26 | Linear/affine/relevant modalities |
| Refinement types | `refinement.js` | 33 | Predicate-refined types ({x:T \| P}) |
| Row polymorphism | `row-poly.js` | 22 | Extensible records, structural subtyping |

### Effects & Control
| Module | File | Tests | Description |
|--------|------|-------|-------------|
| Algebraic effects | `effects.js` | 28 | Perform/Handle, effect handlers |
| Effect types | `effects-types.js` | 35 | Effect rows, handler typing |
| Session types | `session-types.js` | 26 | Typed communication protocols |
| Delimited continuations | `delimited.js` | 22 | shift/reset, first-class continuations |

### Dependent Types & Proofs
| Module | File | Tests | Description |
|--------|------|-------|-------------|
| Inductive types | `inductive.js` | 24 | Bool, Maybe, List, Either, Pair |
| CoC proofs | `coc-proofs.js` | 22 | Leibniz equality, Sigma types |
| Theorem proving | `theorems.test.js` | 28 | De Morgan, associativity, functors |
| Proof assistant | `proof-assistant.js` | 30 | Tactic language (intro, apply, refl) |

### Logic
| Module | File | Tests | Description |
|--------|------|-------|-------------|
| Curry-Howard | `curry-howard.js` | 13 | Propositions as types |

## Total: 381+ tests, 0 failures

## Key Properties Implemented
- **Lambda cube**: Untyped → STLC → System F → CoC
- **Substructural**: Linear, Affine, Relevant, Unrestricted
- **Effects**: Algebraic effects, Delimited continuations, Effect typing
- **Proofs**: Dependent types, Induction, 28 verified theorems
- **Practical**: Gradual types, Row polymorphism, Session types, Refinement types

## Running Tests
```bash
# Run all tests
for f in *.test.js; do node "$f"; done

# Run specific module
node coc.test.js
node effects.test.js
```

## Interactive Tools
```bash
# Proof assistant REPL
node proof-repl.js

# Example session:
# theorem id : Π(A:★).A → A
# intro A
# intro x
# assumption
# qed
```
