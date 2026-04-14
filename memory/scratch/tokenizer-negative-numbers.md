# Tokenizer: Negative Number Ambiguity

uses: 1
created: 2026-04-13
tags: henrydb, tokenizer, parser, bug-pattern

## The Bug

`1-1` was tokenized as `NUMBER(1), NUMBER(-1)` instead of `NUMBER(1), MINUS, NUMBER(1)`.

The tokenizer greedily consumed `-` as part of the next number when it appeared immediately after another token. This broke all arithmetic with subtraction.

## Root Cause

The tokenizer's number recognition looked ahead for `-` followed by digits and consumed both as a single negative number token. It didn't check whether the `-` was binary (subtraction) vs unary (negation).

## The Fix

Only parse `-` as part of a number when the previous token is an operator or the start of input (unary context). If the previous token is a number, identifier, or closing paren, the `-` is a binary operator.

## Prevention

- Tokenizer tests should ALWAYS include expressions like `1-1`, `a-b`, `(1)-2`
- The rule: negation is unary only after operators, `(`, `,`, or at start of input
- This is a classic parsing pitfall — nearly every hand-written tokenizer hits this
