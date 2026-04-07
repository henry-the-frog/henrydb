# Algorithm W — Hindley-Milner Type Inference

uses: 1
created: 2026-04-07
tags: type-inference, hindley-milner, unification, polymorphism

## What It Does
Infers types for an entire program without type annotations. The algorithm behind Haskell, ML, and Rust's type inference.

## Core Steps
1. **Generate fresh type variables** for each expression
2. **Generate constraints** by walking the AST:
   - Variable reference: look up in environment
   - Application `f(x)`: f must have type `α → β`, x must have type `α`, result is `β`
   - Lambda `λx.e`: x gets fresh type var `α`, infer e's type `β`, result is `α → β`  
   - Let `let x = e1 in e2`: infer e1's type, **generalize** it, add to env, infer e2
3. **Unify** constraints using Robinson's unification:
   - `α = Int` → substitute α with Int everywhere
   - `α → β = Int → Bool` → unify α=Int, β=Bool
   - `α = α → Int` → **occurs check** fails (infinite type)
4. **Generalize** let-bound types: free type variables become ∀-quantified (polymorphism)

## Generalization (The Key Insight)
```
let id = λx.x    -- id : ∀α. α → α
in (id 42, id "hello")  -- id used at Int→Int AND String→String
```
Without generalization: `id` gets one concrete type, can't be used polymorphically.
With generalization: free variables in `id`'s type become universally quantified.

## Unification Algorithm
- Maintain substitution map `{α₁ → Int, α₂ → Bool, ...}`
- On `unify(τ₁, τ₂)`: apply current substitution, then:
  - Both concrete and equal → success
  - One is type var → add to substitution (after occurs check)
  - Both are applications `T(a,b) = T(c,d)` → unify pairwise
  - Otherwise → type error

## Complexity
Algorithm W is O(exp(n)) in pathological cases but O(n) in practice.
Almost-linear with union-find optimization for substitutions.
