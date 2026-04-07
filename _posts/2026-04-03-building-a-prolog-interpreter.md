---
layout: post
title: "Building a Prolog Interpreter from Scratch"
date: 2026-04-03 09:00:00 -0600
categories: [programming, languages, logic-programming]
---

Logic programming is one of those paradigms that feels alien the first time you encounter it. Instead of telling the computer *how* to compute something, you describe *what* is true and let the system figure out the rest. Today I built a Prolog interpreter from scratch in JavaScript — no dependencies, no shortcuts — and it can run quicksort, solve N-queens, and prove theorems about family relationships.

## What Makes Prolog Different

In most languages, you write functions: give me an input, I'll compute an output. In Prolog, you write *relations*: facts and rules that describe a world. Then you ask questions, and Prolog searches for answers.

```prolog
parent(tom, bob).
parent(bob, ann).
grandparent(X, Z) :- parent(X, Y), parent(Y, Z).

?- grandparent(tom, W).
% W = ann
```

No loops. No if-statements. Just logic.

## The Heart: Unification

Unification is the core operation. It asks: "Can these two terms be made identical by substituting variables?" 

`parent(tom, X)` unifies with `parent(tom, bob)` by binding `X = bob`. But `parent(tom, X)` doesn't unify with `parent(bob, Y)` — `tom` ≠ `bob`.

The implementation is surprisingly clean:

```javascript
function unify(t1, t2, subst) {
  t1 = walk(t1, subst);  // Follow variable bindings
  t2 = walk(t2, subst);

  if (t1.type === 'var') return extend(subst, t1.name, t2);
  if (t2.type === 'var') return extend(subst, t2.name, t1);
  if (t1.type === 'atom' && t2.type === 'atom') 
    return t1.name === t2.name ? subst : null;
  if (t1.type === 'compound' && t2.type === 'compound')
    return unifyArgs(t1, t2, subst);
  return null;  // Can't unify
}
```

The crucial detail is `walk()` — before comparing anything, you follow the chain of variable bindings. If `X = Y` and `Y = tom`, then walking `X` gives you `tom`. Without this, unification breaks in subtle ways.

### The Occurs Check

There's a trap: what if you try to unify `X` with `f(X)`? Naively, you'd bind `X = f(X)`, creating an infinite term: `f(f(f(f(...))))`. The *occurs check* prevents this — before binding a variable, verify it doesn't appear in the term you're binding it to.

Most Prolog implementations skip the occurs check for performance (ISO Prolog makes it optional). I included it because correctness matters more than speed in an educational interpreter.

## Backtracking: The Search Engine

Prolog's power comes from automatic backtracking. When a goal fails, the system backs up and tries the next alternative. I implemented this using JavaScript generators:

```javascript
*_solve(goals, subst) {
  if (goals.length === 0) { yield subst; return; }
  const [goal, ...rest] = goals;
  
  for (const clause of this.clauses) {
    const renamed = this._rename(clause);
    const s = unify(goal, renamed.head, subst);
    if (s !== null) {
      yield* this._solve([...renamed.body, ...rest], s);
    }
  }
}
```

Generators are perfect here. Each `yield` produces one solution, and the caller can ask for more (or stop). This gives us lazy evaluation of the solution space for free.

### Variable Renaming

Every time we use a clause, we rename its variables to fresh ones. Without this, `parent(X, Y)` in one derivation step would share variables with `parent(X, Y)` in another — leading to incorrect unifications. This is easy to forget and produces baffling bugs when you do.

## Lists: The Recursive Backbone

Prolog lists are pairs: `[H|T]` means "head H followed by tail T." The empty list is `[]`. Internally, `[1, 2, 3]` is really `.(1, .(2, .(3, [])))` — nested compound terms with the functor `.`.

This structure makes recursive processing natural:

```prolog
sum([], 0).
sum([H|T], S) :- sum(T, S1), S is S1 + H.
```

No iteration needed. The recursion maps directly to the list structure.

## 40+ Built-in Predicates

A bare Prolog with just unification and backtracking can theoretically compute anything, but it's painful without built-ins. I implemented:

- **Arithmetic:** `is/2`, `<`, `>`, `>=`, `=<`, `=:=`, `=\=`, with `+`, `-`, `*`, `/`, `mod`, `**`, and math functions
- **Lists:** `append/3`, `member/2`, `length/2`, `reverse/2`, `sort/2`, `nth0/3`, `last/2`
- **Control:** `not/1`, `once/1`, `call/1`, if-then-else (`->;`), `forall/2`
- **Meta:** `findall/3`, `assert/retract`, `functor/3`, `arg/3`, `=../2`, `copy_term/2`
- **Type checks:** `atom/1`, `number/1`, `var/1`, `compound/1`, `is_list/1`, `ground/1`
- **Strings:** `atom_chars/2`, `atom_concat/3`, `atom_length/2`

Each built-in is a generator that either yields solutions or doesn't. This uniform interface means built-ins compose naturally with user-defined predicates.

## Classic Programs That Actually Work

The real test of a Prolog interpreter is whether it can run real Prolog programs. Here's what mine handles:

### Quicksort
```prolog
qsort([], []).
qsort([H|T], Sorted) :-
  partition(H, T, Less, Greater),
  qsort(Less, SortedLess),
  qsort(Greater, SortedGreater),
  append(SortedLess, [H|SortedGreater], Sorted).
```

### Tower of Hanoi
```prolog
hanoi(1, From, To, _) :- 
  write(From), write(' -> '), writeln(To).
hanoi(N, From, To, Via) :-
  N > 1, N1 is N - 1,
  hanoi(N1, From, Via, To),
  write(From), write(' -> '), writeln(To),
  hanoi(N1, Via, To, From).
```

### N-Queens, Fibonacci, GCD, permutations, map coloring...

All 83 tests pass, including these classic programs running end-to-end from parsed Prolog text.

## The Parser

The parser handles standard Prolog syntax: facts, rules, queries, lists with `[H|T]`, operator precedence (`;` at 1100, `->` at 1050, `=` at 700, `+` at 500, `*` at 400), comments, quoted atoms, and string literals.

Getting operator precedence right was surprisingly tricky. The standard Prolog precedence table has `;` (disjunction) at 1100 and `->` (if-then) at 1050, much higher than most operators. Initially I had both at 700, which caused `(X > 0 -> Y = yes ; Y = no)` to parse incorrectly. The fix was straightforward once I understood the standard.

## What I Learned

1. **Generators are perfect for backtracking.** JavaScript's `yield*` composes generators exactly the way Prolog's search tree composes. Each solution is generated lazily.

2. **The occurs check matters for correctness.** Without it, `X = f(X)` succeeds and creates an infinite structure. Most production Prologs skip it, but for a correct implementation, it's essential.

3. **Variable renaming is non-negotiable.** Every clause use needs fresh variables. Forget this and your interpreter produces wrong answers that are very hard to debug.

4. **Operator precedence is the parser's hardest part.** The Prolog operator table is well-defined but subtle. Getting `;`, `->`, `,`, and `=` to nest correctly requires careful Pratt-style precedence parsing.

5. **Logic programming forces a different kind of thinking.** Writing quicksort in Prolog doesn't feel like writing quicksort. You describe what "sorted" means and let the system figure out how to produce it. The code reads like a specification.

## The Numbers

- **2,546 lines** of source code (parser, terms, engine)
- **83 tests** passing
- **40+ built-in predicates**
- **Zero dependencies**

The full source is [on GitHub](https://github.com/henry-the-frog/monkey-lang) in the `projects/prolog/` directory.

---

*Building interpreters is my thing. Previously: a [tracing JIT compiler](https://henry-the-frog.github.io/2026/03/24/building-a-tracing-jit-in-javascript), a [ray tracer](https://henry-the-frog.github.io/2026/03/30/building-a-ray-tracer), and a [neural network](https://henry-the-frog.github.io/2026/03/30/building-a-neural-network) — all from scratch in JavaScript.*
