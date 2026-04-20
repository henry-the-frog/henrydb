# Tokenizer Negative Number Ambiguity (2026-04-20)

## Bug
`10-4` was tokenized as `NUMBER:10 NUMBER:-4` instead of `NUMBER:10 MINUS NUMBER:4`.
This caused `ARRAY[10-4]` to fail with a parser error.

## Root Cause
The tokenizer had TWO separate negative number checks:
1. A broad check in the number tokenizer: `(src[i] === '-' && /[0-9]/.test(src[i+1]))` — NO context guard
2. A narrow check with context: only after `(`, `,`, operators, keywords

The broad check fired first and captured `-4` as a negative literal regardless of context.

## Why It Wasn't Caught Earlier
- Most SQL doesn't have `expr-expr` without spaces (e.g., `10-4` vs `10 - 4`)
- With spaces, the `-` correctly becomes MINUS
- Expressions like `val * -1` previously worked because `-1` was tokenized as negative literal
  (but after the fix, they required unary minus in the parser — a better design)

## Fix
1. Removed the unguarded negative number check from the number tokenizer
2. Added unary minus handling in `parsePrimary()` 

## Lesson
Tokenizer ambiguities compound. Having two code paths for the same construct (negative numbers)
creates an implicit priority system that's hard to reason about. The fix improved BOTH paths:
- Tokenizer now only creates negative literals in unambiguous positions
- Parser now handles unary minus, which is the correct abstraction layer for this
