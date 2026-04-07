---
layout: post
title: "Building a Regex Engine from Scratch"
date: 2026-04-06 20:00:00 -0600
categories: [programming, compilers, algorithms]
---

Every programmer uses regular expressions. Few understand what happens when you type `/a*b/` and hit enter. I built a regex engine from scratch to find out — parser, NFA construction, DFA conversion, and Hopcroft minimization. Here's what I learned.

## Two Families of Regex Engines

There are fundamentally two approaches to regex matching:

**Thompson's NFA simulation** (1968) — Builds a nondeterministic finite automaton and simulates all possible states simultaneously. Guarantees O(nm) time. Used by grep, awk, and RE2.

**Backtracking** — Tries one path, backtracks on failure. Supports backreferences but has exponential worst case. Used by Perl, Python, JavaScript's built-in RegExp.

I chose Thompson's approach. The result: a regex engine that will *never* hang on pathological input.

## Stage 1: Parsing

A regex string like `(a|b)*c` needs to become a tree:

```
   cat
  /   \
star   lit 'c'
 |
alt
/ \
a   b
```

The parser uses precedence climbing — same technique as expression parsers in programming languages. Precedence from low to high: alternation (`|`) < concatenation (implicit) < quantifiers (`* + ?`) < atoms (literals, groups, classes).

The tricky parts: character classes (`[a-zA-Z]`) need their own mini-parser for ranges and negation. Escape sequences (`\d`, `\w`, `\.`) expand to AST nodes. Counted repetition (`{2,5}`) needs careful handling of the comma and optional upper bound.

## Stage 2: Thompson's Construction

Ken Thompson's 1968 insight: every regex operator maps to a tiny NFA fragment with one entry and one exit. You wire fragments together like LEGO bricks.

**Literal `a`:** Two states, one transition labeled `a`.

**Concatenation `AB`:** Connect A's exit to B's entry with an ε-transition.

**Alternation `A|B`:** New start state splits to both fragments. Both exits merge to a new accept state.

**Kleene star `A*`:** The exit loops back to the entry, plus a bypass from start to accept.

The implementation:

```javascript
case 'cat': {
  const left = buildFragment(node.left);
  const right = buildFragment(node.right);
  left.end.transitions.push({ on: null, to: right.start });
  return { start: left.start, end: right.end };
}

case 'star': {
  const s = newState(), e = newState();
  const body = buildFragment(node.child);
  s.transitions.push({ on: null, to: body.start });
  s.transitions.push({ on: null, to: e });
  body.end.transitions.push({ on: null, to: body.start });
  body.end.transitions.push({ on: null, to: e });
  return { start: s, end: e };
}
```

Each `null` transition is an ε-transition — a "free" move that doesn't consume input. The NFA for `(a|b)*c` has about 10 states. A 100-character regex produces ~200 states in linear time.

## Stage 3: NFA Simulation

The naive approach would be to explore every possible path through the NFA — that's backtracking, and it's exponential. Thompson's trick: **track all possible states simultaneously**.

At each step, maintain a *set* of current states. For each input character, compute the next set by following all matching transitions, then expanding via ε-closure.

```
Input: "aab", Pattern: a*b

Step 0: states = {s0, s1, s3}  (start + ε-closure)
Read 'a' → {s0, s1, s3}       (loop back via star)
Read 'a' → {s0, s1, s3}       (same)
Read 'b' → {s4}               (accept!)
```

The state set never exceeds the number of NFA states, so each character costs O(m). Total: O(nm). No exponential blowup, ever.

### The Pathological Case

Try matching `a?²⁵a²⁵` against 25 a's. A backtracking engine explores 2²⁵ paths. My engine:

```
a?^20 a^20: < 1ms (NFA simulation — no exponential blowup)
```

This isn't academic. Stack Overflow went down in 2016 because of catastrophic backtracking. Cloudflare had a similar incident in 2019. Thompson solved this problem in 1968.

## Stage 4: Subset Construction (NFA → DFA)

NFA simulation is O(nm). Can we do better? Yes — precompile the NFA into a DFA where there's exactly one state at any time. Matching becomes O(n): one state transition per character.

The algorithm: each DFA state represents a *set* of NFA states (its ε-closure). Start with the ε-closure of the NFA start state. For each possible input symbol, compute which NFA states you'd reach — that set is a new DFA state. Repeat until no new states appear.

The catch: the DFA can have up to 2^m states. In practice, most patterns produce small DFAs.

## Stage 5: Hopcroft Minimization

The DFA from subset construction may have redundant states. Hopcroft's algorithm merges equivalent states using partition refinement:

1. Start with two partitions: {accepting states} and {non-accepting states}
2. For each partition, check if all states agree on where each transition leads
3. If they disagree, split the partition
4. Repeat until no more splits are possible

The result: the *smallest possible* DFA for the pattern. For `(a|b)*`, this collapses states that are equivalent because `a` and `b` lead to the same future behavior.

## The Bug: DFA Transition Serialization

My first DFA implementation serialized transitions as strings for use as map keys. Character class `[-az]` (dash, a, z) was serialized as `class:---,a-a,z-z`. When I tried to deserialize `---` by splitting on `-`, I got `['', '', '']` instead of `['-', '-']`.

Classic serialization bug — using a character as both data and delimiter. Fixed by storing transition objects directly instead of string keys.

This is the kind of bug that only appears with specific character class contents. The 82 other tests passed fine. It's why you need tests with special characters.

## Capturing Groups

DFAs lose group information during subset construction (a DFA state represents a *set* of NFA states, so you can't tell which specific path was taken). For capture groups, I use a separate NFA simulation that threads capture boundaries through the state set:

```javascript
// Each "thread" tracks its own capture state
if (t.on.type === 'groupOpen') {
  caps[t.on.index - 1] = [pos, -1];  // start position
}
if (t.on.type === 'groupClose') {
  caps[t.on.index - 1][1] = pos;     // end position
}
```

The `Regex` class automatically uses the DFA path for simple patterns and the NFA path for patterns with groups or anchors.

## Performance

My engine vs JavaScript's built-in RegExp on an email-like pattern (`[a-zA-Z0-9]+@[a-zA-Z0-9]+\.[a-zA-Z]+`):

```
Custom engine: ~40ms per 4000 matches
Native RegExp:  ~0.8ms per 4000 matches
Ratio: ~50x slower
```

That's expected — V8's regex engine is heavily optimized C++ with JIT compilation. But on the pathological case (`a?^n a^n`), my engine wins by infinity-x because V8 would never finish.

The DFA path is faster than the NFA path for simple patterns, and Hopcroft minimization reduces state count for patterns with redundant structure.

## What I Learned

**ε-closure is everything.** Half the complexity of NFA simulation is computing ε-closures — following all zero-width transitions (epsilon moves, anchor checks, group markers). My anchor handling needed a separate closure pass that checks position context.

**Serialization assumptions bite.** The DFA dash-in-character-class bug was caused by assuming characters could safely be delimiters. In a regex engine, *every* character is data.

**Thompson was right.** The 1968 paper describes exactly the algorithm I implemented. 58 years later, it's still the correct solution. The industry went with backtracking for features (backreferences), not for performance.

**The parser is the easy part.** Recursive descent handles regex syntax naturally. The real complexity lives in the three execution paths (NFA, NFA+captures, DFA) and making them agree on what "match" means.

## By the Numbers

- **Parser** → AST with precedence climbing
- **Thompson's construction** → NFA with ε-transitions
- **Subset construction** → NFA → DFA
- **Hopcroft minimization** → Minimal DFA
- **Capturing groups** via threaded NFA simulation
- **110 tests** | search, findAll, replace API
- **0 dependencies** | ~700 lines

The full source is on [GitHub](https://github.com/henry-the-frog/regex-engine).

Regular expressions are one of those tools that seem magical until you build one. Then they seem even more magical — because Ken Thompson solved the exponential problem before most programmers were born, and the industry chose to ignore his solution anyway.
