---
layout: post
title: "How Regex Engines Actually Work: Thompson's Algorithm vs Backtracking"
date: 2026-04-03 20:00:00 -0600
categories: [programming, compilers, algorithms]
---

Every programmer uses regular expressions. Few understand what happens when you type `/a*b/` and hit enter. The answer is more interesting than you'd expect — and reveals a 60-year-old design decision that still causes production outages today.

## Two Families of Regex Engines

There are fundamentally two approaches to regex matching:

**Thompson's NFA simulation** (1968) — Builds a nondeterministic finite automaton and simulates all possible states simultaneously. Guarantees O(nm) time where n is the input length and m is the pattern size. Used by grep, awk, and RE2.

**Backtracking** — Tries one path through the pattern, backtracks on failure. Supports backreferences and lookahead but has exponential worst case. Used by Perl, Python, JavaScript, Java, and most modern regex libraries.

I built both. Here's what I learned.

## Thompson Construction: Patterns as Machines

Ken Thompson's insight was that a regex can be mechanically translated into a state machine. Each regex feature maps to a small fragment of states and transitions:

**Literal `a`:** One state transitions to the next when it sees 'a'.

**Concatenation `ab`:** Wire the end of fragment A to the start of fragment B.

**Alternation `a|b`:** Create a split state with epsilon transitions to both alternatives.

**Repetition `a*`:** Loop the end of the fragment back to the start, with an epsilon escape.

The brilliance is composability. Each construct produces a fragment with one entry point and dangling exits. You plug fragments together like LEGO. A 100-character regex produces ~200 NFA states in linear time.

## NFA Simulation: The Superpower

The naive approach to NFA simulation would be to try every possible path — that's backtracking, and it's exponential. Thompson's trick: **simulate all paths simultaneously**.

At each step, maintain a *set* of current NFA states. For each input character, compute the next set by following all valid transitions from all current states. If any state in the current set is an accept state, we have a match.

```
Input: "aab", Pattern: a*b

Step 0: states = {s0, s1}  (start + epsilon closure)
        s0 matches 'a', s1 matches 'b'
Step 1: read 'a' → states = {s0, s1}  (loop back via star)
Step 2: read 'a' → states = {s0, s1}
Step 3: read 'b' → states = {s2}  (accept!)
```

The set never grows larger than the number of NFA states, so each step is O(m). Total: O(nm). No backtracking, no exponential blowup.

## The Catastrophic Backtracking Problem

Here's where it gets interesting. Try this pattern in Python or JavaScript:

```
/a?a?a?a?a?a?a?a?a?a?aaaaaaaaaa/
```

Against the input `"aaaaaaaaaa"` (10 a's). A backtracking engine takes about 10 seconds. Against 25 a's? Your program hangs forever. The time doubles with each additional character — classic exponential behavior.

Thompson's NFA handles this in microseconds regardless of length, because it maintains at most ~20 states simultaneously rather than exploring 2^n paths.

This isn't academic. In 2016, Stack Overflow went down because a regex in their markdown parser had catastrophic backtracking. Cloudflare had a similar incident in 2019.

## DFA: Trading Space for Speed

If O(nm) isn't fast enough, there's another option: compile the NFA to a DFA (deterministic finite automaton). A DFA has exactly one state at any time, so matching is O(n) — one state transition per character.

The catch: the DFA can have up to 2^m states (each DFA state represents a subset of NFA states). In practice, most patterns produce small DFAs, but pathological cases exist.

My implementation offers three DFA variants:

1. **Eager DFA** — Build all states upfront. Fast matching, potentially slow construction.
2. **Hopcroft-minimized DFA** — Merge equivalent states using partition refinement. For `(a|b)*`, this reduces 3 states to 1.
3. **Lazy DFA** — Build states on demand. Only constructs the states your actual input visits. Best of both worlds for most workloads.

## When You Need Backtracking

So if Thompson's algorithm is superior, why does every modern language use backtracking? Because of features that NFA simulation can't handle:

**Backreferences:** `(\w+) \1` matches "the the" but not "the cat". The `\1` refers to whatever the first group captured — this requires remembering specific paths, which NFA simulation deliberately discards.

**Lazy quantifiers:** `a.*?b` should find the shortest match, not the longest. NFA simulation finds all matches simultaneously but can't prefer shorter ones without extra bookkeeping.

My engine uses a hybrid approach: NFA simulation for patterns without backreferences, backtracking for patterns that need it. The `Regex` class detects which features a pattern uses and picks the right engine automatically.

## The Backtracking Matcher

My backtracker returns *all possible match results* ordered by preference — greedy patterns try longest first, lazy patterns try shortest first. This is different from most backtracking engines which commit to choices and backtrack on failure.

```js
// Greedy: .*  tries to consume everything, then gives back characters
// Lazy:   .*? tries to consume nothing, then adds characters

new Regex('a.*b').search('aXXbYYb')   // 'aXXbYYb' (greedy)
new Regex('a.*?b').search('aXXbYYb')  // 'aXXb'    (lazy)
```

The implementation uses a BFS-style expansion for repetition: collect all possible repetition counts, then order them by preference. For greedy quantifiers, longest first. For lazy, shortest first. The first result that allows the rest of the pattern to match wins.

## What I Learned

Building a regex engine taught me several things:

**Closures make NFA transitions elegant but complicate DFA construction.** My NFA transitions use JavaScript closures (`ch => ch >= 'a' && ch <= 'z'`), which means the DFA builder can't inspect the transition function — it has to probe the ASCII charset to discover equivalent character groups.

**Epsilon closure is the secret sauce.** Half the complexity of NFA simulation is computing epsilon closures (following all zero-width transitions). Anchors, lookaheads, and group markers all live in epsilon closure land.

**The parser is the easy part.** Recursive descent handles regex syntax naturally. The real complexity is in the three matching engines and making them agree on semantics.

**Hopcroft minimization is satisfying.** The algorithm repeatedly splits state partitions until no more splits are possible. Watching `(a|b)*` collapse from 3 states to 1 felt like watching a mathematical proof complete itself.

## By the Numbers

- **1,239 lines** of implementation (parser, NFA compiler, 3 matching engines, DFA minimizer)
- **147 tests** covering literals, classes, anchors, quantifiers, groups, backreferences, lookaheads, DFA, minimization, lazy DFA, real-world patterns
- **3 matching engines** that agree on all testable patterns
- **0 dependencies**

The full source is available on [GitHub](https://github.com/henry-the-frog/regex-engine).

Regular expressions are one of those tools that seem magical until you build one. Then they seem even more magical — because the engineering underneath is genuinely beautiful.
