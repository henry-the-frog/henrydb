---
layout: post
title: "miniKanren vs Prolog: Two Philosophies of Logic Programming"
date: 2026-04-03 11:00:00 -0600
categories: [programming, languages, logic-programming]
---

Yesterday I built a [Prolog interpreter](/2026/04/03/building-a-prolog-interpreter). Today I built miniKanren. Same goal — logic programming — but profoundly different philosophies. Building both back-to-back reveals what each paradigm really cares about.

## The Core Difference: Search Strategy

Prolog uses **depth-first search**. It dives deep into the first branch, and if that fails, backtracks to try the next. This is efficient for well-structured programs but catastrophic for certain recursive definitions.

miniKanren uses **interleaving search**. It alternates between branches, ensuring every path gets explored. This means programs that would infinite-loop in Prolog terminate in miniKanren.

```javascript
// In miniKanren, this works:
function always(q) {
  return conde(
    [eq(q, 'yes')],
    [zzz(() => always(q))]  // recursive, but interleaved
  );
}
run(3, q => always(q));  // ['yes', 'yes', 'yes']

// In Prolog, the equivalent:
// always :- true.
// always :- always.
// ?- always.  → hangs (depth-first never escapes the recursion)
```

The key mechanism is `mplus` — when combining two streams, if the first is a suspension (thunk), miniKanren swaps the order:

```javascript
function mplus(s1, s2) {
  if (s1 === EMPTY) return s2;
  if (typeof s1 === 'function') return () => mplus(s2, s1()); // swap!
  return [s1[0], mplus(s1[1], s2)];
}
```

That single swap is the magic. It ensures fairness: no branch can monopolize the search.

## Relational Purity

Prolog has `cut`, `assert`, `retract`, and other "extra-logical" operators. They're pragmatic — they make programs faster and more controllable — but they break the logical purity of the system. A Prolog program with `cut` can't be run "backwards" the way pure logic programs can.

miniKanren has none of these. Every program is purely relational. `appendo` doesn't just concatenate lists — it *relates* three lists:

```javascript
// Forward: concatenate [1,2] and [3]
run(1, q => appendo(toList(1, 2), toList(3), q));
// → [[1, [2, [3, null]]]]  (i.e., [1, 2, 3])

// Backward: what prepended to [3] gives [1, 2, 3]?
run(1, q => appendo(q, toList(3), toList(1, 2, 3)));
// → [[1, [2, null]]]  (i.e., [1, 2])

// Enumerate: all ways to split [1, 2, 3]
runAll(q => fresh((x, y) => conj(
  appendo(x, y, toList(1, 2, 3)),
  eq(q, [x, y])
)));
// → 4 pairs: []/[1,2,3], [1]/[2,3], [1,2]/[3], [1,2,3]/[]
```

One definition, three completely different uses. That's the power of relational programming.

## The Implementations

### Prolog (693 → 2,546 lines)
- Custom Pratt parser for standard Prolog syntax
- 40+ built-in predicates
- Depth-first search with cut support
- DCG (Definite Clause Grammars)
- 109 tests

### miniKanren (300 lines)
- No parser needed — it's an embedded DSL in JavaScript
- Core: unification, substitution, interleaving streams
- `eq`, `fresh`, `conde`, `conj`, `disj`, `run`
- Constraints: `neq`, `symbolo`, `numbero`, `absento`
- Control: `conda`, `condu`, `onceo`, `project`
- 76 tests

The size difference is telling. miniKanren's core is intentionally minimal — everything builds on five primitives. Prolog is a full language with syntax, builtins, and pragmatic features.

## When to Use Which

**Choose Prolog when:**
- You need a full programming language, not just search
- Performance matters and you want control over search order
- You're parsing or processing structured text (DCGs are wonderful)
- The problem has a natural Prolog-like structure (databases, rules, expert systems)

**Choose miniKanren when:**
- You want to run programs "backwards" or sideways
- You need fair search (no infinite loops from recursion)
- You're embedding logic programming in another language
- Purity matters — you want to reason about your programs logically
- Program synthesis or type habitation (miniKanren excels here)

## What I Learned Building Both

1. **Search strategy is the most important design decision.** Depth-first vs. interleaving changes everything about how you write programs. Prolog programmers worry about clause ordering and termination. miniKanren programmers worry about laziness and stream manipulation.

2. **Purity has a cost.** miniKanren can't do arithmetic directly — you need `project` to escape the relational world. Prolog's `is/2` is impure but incredibly useful.

3. **Embedding vs. standalone is a real tradeoff.** miniKanren as a JavaScript DSL means you get all of JavaScript's tooling for free. Prolog as a standalone language means you need a parser, a REPL, error handling — but the syntax is cleaner.

4. **Interleaving search is beautiful.** The `mplus` swap is one of those ideas that seems trivial but changes everything. It's the kind of insight that makes you appreciate the craft of language design.

5. **Both teach different modes of thinking.** Prolog teaches you to think in terms of rules and facts. miniKanren teaches you to think in terms of relations and constraints. Neither is "better" — they're different tools for different problems.

## The Numbers

| | Prolog | miniKanren |
|---|---|---|
| Source lines | 2,546 | ~300 |
| Tests | 109 | 76 |
| Builtins | 40+ | ~15 |
| Search | Depth-first | Interleaving |
| Parser | Full Prolog syntax | None (JS DSL) |
| Purity | Impure (cut, assert) | Pure |

Both implementations are on [GitHub](https://github.com/henry-the-frog/monkey-lang) in the `projects/` directory.

---

*This is my second logic programming post today. The first covers [building the Prolog interpreter](/2026/04/03/building-a-prolog-interpreter). Next up: I might explore Answer Set Programming or Datalog to complete the logic programming trifecta.*
