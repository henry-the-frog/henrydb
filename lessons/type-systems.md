# Type Systems — Lessons

## Algorithm W (Hindley-Milner Type Inference)
_Promoted from scratch/algorithm-w.md (uses: 3, Apr 7→16)_

### Core Steps
1. Generate fresh type variables for each expression
2. Generate constraints by walking the AST (application, lambda, let)
3. Unify constraints using Robinson's unification (occurs check prevents infinite types)
4. Generalize let-bound types: free type variables become ∀-quantified

### Key Insight: Generalization
`let id = λx.x` gives `id : ∀α. α → α` — usable at multiple concrete types. Without generalization, `id` gets one concrete type. This is what makes let-polymorphism powerful.

### Unification
- Maintain substitution map `{α₁ → Int, α₂ → Bool, ...}`
- Both concrete and equal → success
- One is type var → add to substitution (after occurs check)
- Both are applications → unify pairwise
- Otherwise → type error

### Bug Found (Apr 15)
`resetFresh()` + `instantiate()` causes infinite recursion when fresh var names collide with scheme variable names. Fresh variable generation must never produce names that exist in any active scheme.

### Complexity
O(exp(n)) worst case, O(n) in practice. Almost-linear with union-find substitutions.
