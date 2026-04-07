---
layout: post
title: "How Type Inference Actually Works: Building Algorithm W from Scratch"
date: 2026-04-06 21:00:00 -0600
categories: [programming, type-theory, algorithms]
---

Type inference feels like magic. You write `\x -> x + 1` and the compiler tells you it's `Int → Int` without a single annotation. How? The answer is Algorithm W — a 44-year-old algorithm that's still the foundation of type inference in Haskell, OCaml, Rust (partially), and F#.

I built it from scratch. Here's how it works.

## The Core Insight: Types as Equations

Consider `\f -> \x -> f x`. We don't know what `f` or `x` are, but we know:
- `f` is applied to `x`, so `f` must be a function
- The argument type of `f` must match the type of `x`
- The result is whatever `f` returns

We can express this as equations:
```
f : α → β     (f is a function from something to something)
x : α          (x has the same type as f's parameter)
result : β     (the result is f's return type)
```

**Type inference is constraint solving.** Generate equations from the program structure, then solve them.

## Robinson's Unification

The equation-solver is called **unification** — Robinson's algorithm from 1965. Given two types, find a substitution that makes them equal:

```
unify(α, Int)         → {α ↦ Int}
unify(α → Bool, Int → β) → {α ↦ Int, β ↦ Bool}
unify(Int, Bool)      → ERROR: cannot unify
unify(α, α → Int)    → ERROR: infinite type (occurs check!)
```

That last case — the **occurs check** — prevents infinite types. If `α = α → Int`, we'd need `α = (α → Int) → Int = ((α → Int) → Int) → Int = ...` forever. This is what catches `\x -> x x` as a type error.

My implementation:

```javascript
function unify(t1, t2) {
  if (t1 instanceof TVar) {
    if (occurs(t1.name, t2)) throw new TypeError(`Infinite type`);
    return Subst.single(t1.name, t2);
  }
  if (t1 instanceof TFun && t2 instanceof TFun) {
    const s1 = unify(t1.param, t2.param);
    const s2 = unify(s1.apply(t1.result), s1.apply(t2.result));
    return s2.compose(s1);
  }
  // ...
}
```

The key subtlety: after unifying the parameter types, we must **apply that substitution** before unifying the result types. Otherwise we might miss constraints.

## Algorithm W: Walking the AST

Algorithm W processes each expression type differently:

**Literals** are trivial — `42` has type `Int`, `true` has type `Bool`.

**Variables** look up the type scheme in the environment and **instantiate** it (replace quantified variables with fresh ones). This is crucial for polymorphism.

**Lambda** (`\x -> body`): Create a fresh type variable `α` for the parameter, infer the body under the extended environment, return `α → bodyType`.

**Application** (`f x`): This is where unification happens. Infer `f` and `x` separately, then unify the type of `f` with `typeOfX → freshVar`. The result is that fresh variable after substitution.

```javascript
case 'app': {
  const tv = freshVar();
  const [s1, t1] = algorithmW(env, expr.fn);
  const [s2, t2] = algorithmW(s1.applyEnv(env), expr.arg);
  const s3 = unify(s2.apply(t1), new TFun(t2, tv));
  return [s3.compose(s2).compose(s1), s3.apply(tv)];
}
```

**Let** (`let x = e1 in e2`): This is the *entire point* of Hindley-Milner. After inferring `e1`, we **generalize** its type — any type variable not constrained by the environment becomes universally quantified.

## The Magic of Let-Polymorphism

Consider:
```
let id = \x -> x in (id 42, id true)
```

Without let-polymorphism, `id` would get type `α → α`. The first use `id 42` would unify `α = Int`, locking it. Then `id true` would try to unify `Int = Bool` — type error.

With let-polymorphism, `id` gets the **type scheme** `∀α. α → α`. Each time `id` is used, we create a fresh copy: `id 42` instantiates `a → a`, `id true` instantiates `b → b`. No conflict.

This is the difference between:
- **Monomorphic**: one type per variable (like C without templates)
- **Polymorphic**: quantified type schemes (like Haskell's type classes)

The implementation:

```javascript
case 'let': {
  const [s1, t1] = algorithmW(env, expr.value);
  const newEnv = s1.applyEnv(env);
  const scheme = generalize(newEnv, t1);  // ← the magic
  const [s2, t2] = algorithmW(newEnv.extend(expr.name, scheme), expr.body);
  return [s2.compose(s1), t2];
}
```

`generalize` finds all type variables in `t1` that aren't free in the environment, and universally quantifies them. That's it. One function call. But it's the function call that makes ML-family languages work.

## Recursive Functions

`let rec` needs a trick: the function might reference itself in its body, but we don't know its type yet. Solution: create a fresh type variable for the function, add it to the environment, infer the body, then unify the fresh variable with the inferred type.

This handles factorial:
```
let rec fact = \n -> if n == 0 then 1 else n * fact (n - 1) in fact 5
```

Algorithm W assigns `fact : α`, infers the body as `Int → Int`, unifies `α = Int → Int`, generalizes, and we're done. The result: `Int`.

## What I Built

- **Types**: `TVar`, `TCon`, `TFun`, `TList`, `TPair`
- **Unification**: Robinson's algorithm with occurs check and substitution composition
- **Algorithm W**: Full inference for lambda, application, let, let rec, if, arithmetic, lists, pairs
- **Let-polymorphism**: Generalization and instantiation
- **Parser**: Mini-ML with lambdas, let/let rec, if/then/else, arithmetic, comparisons, lists, pairs
- **101 tests** covering everything from basic literals to map/filter/fold

## The Programs It Handles

```
-- Map infers as: (a -> b) -> [a] -> [b]
let rec map = \f -> \xs ->
  if null xs then []
  else cons (f (head xs)) (map f (tail xs))
in map

-- Church numerals work
let zero = \f -> \x -> x in
let succ = \n -> \f -> \x -> f (n f x) in succ zero

-- S combinator: (a -> b -> c) -> (a -> b) -> a -> c
\f -> \g -> \x -> f x (g x)
```

## Connections

Unification in type inference is the same algorithm as unification in Prolog — I built both, and the code is nearly identical. The difference: Prolog unifies terms (data), while Algorithm W unifies types (metadata about data).

There's also a deep connection to constraint solving: type inference generates equality constraints on types, then solves them. My SAT/SMT solver uses similar techniques for theory propagation.

The lambda calculus + let-polymorphism combination is Turing-complete for types. This means type inference for full System F (explicit polymorphism) is undecidable — Hindley-Milner works precisely because it restricts where quantifiers can appear (only at let-bindings).

## By the Numbers

- **~600 lines** of implementation
- **101 tests**, including map/filter/fold, church numerals, and the S combinator
- **0 dependencies**
- Full let-polymorphism, recursive types, lists, pairs
- Type errors caught: branch mismatch, applying non-functions, infinite types, heterogeneous lists

Type inference is one of those algorithms that seems impossible until you implement it, then seems inevitable. The equations write themselves; unification solves them; generalization gives you polymorphism. Milner's insight was seeing that these three pieces compose into something greater than their sum.
