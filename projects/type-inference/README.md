# Type Inference

Hindley-Milner type inference (Algorithm W) built from scratch in JavaScript.

**Zero dependencies. Pure type theory.**

## What It Does

Give it an expression, get back its most general type — with no type annotations required:

```javascript
const { typeOf } = require('./types.js');

typeOf('42')                           // → "Int"
typeOf('\\x -> x')                     // → "a -> a"  (polymorphic identity)
typeOf('\\x -> x + 1')                 // → "Int -> Int"
typeOf('let id = \\x -> x in (id 42, id true)')  // → "(Int, Bool)"  ← let-polymorphism!
```

That last one is the magic: `id` is used at two different types (`Int → Int` and `Bool → Bool`) in the same expression, and the type checker figures it out.

## Architecture

```
Source → [Tokenizer] → Tokens → [Parser] → AST → [Algorithm W] → Type
                                                        ↕
                                                   Unification
                                                 (Robinson's algorithm)
```

### Core Components:

1. **Types** — `TVar` (α), `TCon` (Int), `TFun` (τ₁ → τ₂), `TList` ([τ]), `TPair` ((τ₁, τ₂))
2. **Substitution** — Maps type variables to types, with composition
3. **Unification** — Robinson's algorithm with occurs check
4. **Algorithm W** — Walks the AST, generates constraints, solves via unification
5. **Generalize/Instantiate** — The key to let-polymorphism

## Language (Mini-ML)

```
-- Literals
42, true, false, "hello"

-- Lambda and application
\x -> x + 1
(\x -> x) 42

-- Let bindings (with polymorphism!)
let id = \x -> x in id 42

-- Recursive functions
let rec fact = \n -> if n == 0 then 1 else n * fact (n - 1) in fact 5

-- If-then-else
if x < 10 then x else x - 10

-- Lists
[1, 2, 3]
head xs, tail xs, cons 1 xs, null xs, length xs

-- Pairs
(1, true)
fst p, snd p

-- Arithmetic: + - * / %
-- Comparison: == != < > <= >=
-- String: ++ (concatenation)
-- Boolean: not
```

## Key Algorithm: How Algorithm W Works

For each expression type, Algorithm W produces a substitution and a type:

| Expression | Rule |
|---|---|
| `42` | Return `(∅, Int)` — no constraints |
| `x` | Look up `x` in environment, instantiate its scheme |
| `λx.e` | Create fresh `α` for `x`, infer `e` under `x:α`, return `α → τe` |
| `f a` | Infer `f` and `a` separately, unify `τf` with `τa → β`, return `β` |
| `let x = e₁ in e₂` | Infer `e₁`, **generalize** its type, infer `e₂` under `x : ∀ᾱ.τ₁` |

The critical step is **generalization at let-bindings**: type variables that aren't constrained by the environment become universally quantified. This is what lets `id = λx.x` have type `∀α. α → α` instead of being locked to one type.

## Programs It Type-Checks

```
-- Factorial
let rec fact = \n -> if n == 0 then 1 else n * fact (n - 1) in fact 5
-- Inferred: Int

-- Map
let rec map = \f -> \xs -> if null xs then [] else cons (f (head xs)) (map f (tail xs))
in map (\x -> x + 1) [1, 2, 3]
-- Inferred: [Int]

-- Polymorphic identity (the classic test)
let id = \x -> x in (id 42, id true)
-- Inferred: (Int, Bool)

-- S combinator
\f -> \g -> \x -> f x (g x)
-- Inferred: (a -> b -> c) -> (a -> b) -> a -> c
```

## Type Errors It Catches

```
if true then 1 else false     → Cannot unify Int with Bool
1 2                            → Cannot unify Int with a -> b
true + 1                       → Cannot unify Bool with Int
\x -> x x                     → Infinite type (occurs check)
[1, true]                      → Cannot unify Int with Bool
```

## Tests

```
101 tests | 0 failures
```

Covers: type system primitives, substitution, unification, occurs check, free variables, generalization/instantiation, parser, literal inference, arithmetic, lambda, application, let-polymorphism, recursion, if-expressions, lists, pairs, type errors, and complex programs (map, filter, fold, church numerals, combinators).

## Files

```
types.js  — Types, unification, Algorithm W, parser
test.js   — 101 tests
README.md — This file
```

## References

- Damas & Milner, "Principal type-schemes for functional programs" (1982)
- Milner, "A Theory of Type Polymorphism in Programming" (1978)
- Pierce, "Types and Programming Languages" (2002), Ch. 22
