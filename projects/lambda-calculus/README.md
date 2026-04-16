# Lambda Calculus Interpreter

Pure untyped lambda calculus interpreter in JavaScript. Zero dependencies.

## Features

- **Parser** — `λx.body` and `\x.body` syntax, multi-param lambdas, left-associative application
- **4 Reduction Strategies** — Normal-order, applicative-order, call-by-value, call-by-name
- **De Bruijn Indices** — Convert to/from de Bruijn representation
- **Alpha-Equivalence** — Structural equality up to renaming
- **Capture-Avoiding Substitution** — Correct handling of variable capture
- **Church Encodings** — Booleans, numerals, pairs, lists
- **Fixed-Point Combinators** — Y and Z combinators
- **Step-by-Step Tracing** — Full reduction trace for debugging
- **Pretty Printing** — Minimal and verbose output modes

## Church Encodings

```
TRUE  = λt f.t
FALSE = λt f.f
ZERO  = λf x.x
SUCC  = λn f x.f (n f x)
PLUS  = λm n f x.m f (n f x)
MULT  = λm n f.m (n f)
PAIR  = λa b f.f a b
Y     = λf.(λx.f (x x)) (λx.f (x x))
```

## Reduction Strategies

| Strategy | Evaluates under λ? | Evaluates argument? | Finds normal form? |
|----------|-------------------|--------------------|--------------------|
| Normal-order | ✅ | After func | Always (if exists) |
| Applicative | ✅ | Before func | May diverge |
| Call-by-value | ❌ | Before beta | Weak head NF |
| Call-by-name | ❌ | Never | Weak head NF |

Normal-order is the only strategy guaranteed to find a normal form if one exists (Church-Rosser theorem).

## Tests

```bash
node --test lambda.test.js
```

**104 tests** covering tokenizer, parser, free variables, substitution, all 4 reduction strategies, de Bruijn indices, alpha-equivalence, Church encodings (booleans, numerals, pairs), combinators (S, K, I, B, C, W, Y), and complex programs.
