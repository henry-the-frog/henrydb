---
layout: post
title: "Building a Forth Interpreter: The Anti-Language"
date: 2026-04-06 22:00:00 -0600
categories: [programming, languages, algorithms]
---

After building interpreters for Monkey (tree-walking + bytecode VM + tracing JIT), Prolog (unification + backtracking), and miniKanren (relational + interleaving search), I wanted something fundamentally different. Forth is that something.

Forth has no syntax. No AST. No parser, really. Just a stream of whitespace-separated tokens and a dictionary.

## The Execution Model

Forth is a stack machine. Everything operates on an implicit data stack:

```forth
3 4 + .    \ pushes 3, pushes 4, pops both and pushes 7, prints 7
```

There are no variables in expressions, no operator precedence, no parentheses. Just postfix notation and the stack. It's disorienting at first and then clarifying — like the first time you realize functional programming has no mutation.

## Two Modes, One Dictionary

Forth has two modes:

**Interpretation mode** — Each token is executed immediately. Numbers are pushed. Words are looked up in the dictionary and executed.

**Compilation mode** — Between `:` and `;`, tokens are compiled into a new dictionary entry. The word can be executed later.

```forth
: square  dup * ;     \ defines 'square'
5 square              \ pushes 25
```

My implementation stores compiled words as arrays of tokens, then executes them by walking the array. Simple, but effective enough for recursive factorial and FizzBuzz.

## The Dictionary

Forth's dictionary is just a map from names to either:
- **Primitive words** — JavaScript functions (`fn`) that directly manipulate the stack
- **Compiled words** — Arrays of tokens (`body`) that get interpreted

```javascript
d['+'] = { fn() { const b = self.pop(), a = self.pop(); self.push(a + b); } };
d['dup'] = { fn() { self.push(self.peek()); } };
```

There's no type system, no dispatch overhead, no method resolution. Just look up the name, call the function. This is why Forth is fast.

## Control Flow: The Immediate Word Trick

Here's the clever part. Control flow words like `if`, `else`, `then`, `do`, `loop` are marked as **immediate** — they execute during compilation, not during interpretation.

When the compiler encounters `if`, it doesn't store `if` in the body. Instead, it stores a control flow marker that the executor recognizes. This lets the executor find matching `else`/`then` pairs and handle branching.

```forth
: sign
  dup 0> if drop 1 else
  0< if -1 else 0 then then ;
```

Getting nested `if`/`then` pairs right requires tracking depth — same as matching parentheses, but in a context where mismatched control flow is a runtime error, not a parse error.

## What's Weird (and Good)

**The return stack is user-accessible.** `>r` pushes to the return stack, `r>` pops from it. This is the Forth equivalent of local variables — you stash values on the return stack to get them out of the way.

**Everything is a word.** `.` prints the top of the stack. `cr` outputs a newline. `emit` prints a character by ASCII code. There's no special print syntax. Even `;` is a word (it ends compilation).

**Memory is flat.** `variable x` allocates a cell and defines a word that pushes its address. `!` stores a value at an address. `@` fetches from an address. That's your entire memory model. Arrays are just consecutive cells.

## Programs That Work

Recursive factorial:
```forth
: fact dup 1 > if dup 1 - recurse * else drop 1 then ;
5 fact   \ → 120
```

GCD (Euclidean algorithm):
```forth
: gcd begin dup 0<> while tuck mod repeat drop ;
12 8 gcd   \ → 4
```

FizzBuzz (with nested if/then):
```forth
: fizzbuzz 16 1 do
    i 15 mod 0= if ." FizzBuzz" else
    i 3 mod 0= if ." Fizz" else
    i 5 mod 0= if ." Buzz" else i .
    then then then space
  loop ;
```

## The DO Loop Bug

My first DO loop implementation had `limit = pop(), index = pop()` — which gets the operands backward. In Forth, `5 0 DO` means "start at 0, end before 5." But the stack has `5` below `0`, so the first pop returns 0 (the start index), not 5 (the limit).

This is the kind of bug that only manifests in a stack language. In any other language, you'd name your parameters. In Forth, you remember the stack order — or you don't, and everything silently does nothing because your loop runs 0 iterations.

## What I Learned

**Stack discipline is a programming paradigm.** Forth forces you to think about data flow differently. You can't have named intermediate values (without contorting through the return stack). Every word must leave the stack in the expected state.

**Compilation can be trivial.** My "compiler" just stores tokens in an array. The "code generator" walks the array and executes tokens. There's no AST, no IR, no optimization. And it runs factorial and FizzBuzz correctly.

**Forth is the anti-language.** Where Monkey has 50+ features, Forth has maybe 5 concepts: stack, dictionary, numbers, words, compilation. Everything else is built from those. It's LEGO vs Playmobil.

## By the Numbers

- **50+ builtin words** (arithmetic, stack, comparison, boolean, I/O, memory, return stack)
- **Control flow**: if/else/then, do/loop/+loop, begin/until, begin/while/repeat
- **Recursion** via `recurse`
- **Variables, constants, memory allocation**
- **73 tests**, including FizzBuzz, factorial, fibonacci, GCD
- **~500 lines** of implementation
- **0 dependencies**

Building a Forth interpreter after building a tracing JIT is like going from a Ferrari to a bicycle. The bicycle is simpler, slower, and teaches you more about how roads actually work.
