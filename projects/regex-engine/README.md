# Regex Engine

A regex engine built from scratch in JavaScript — from parsing through Thompson's NFA construction to subset construction (NFA→DFA) and Hopcroft minimization.

**Zero dependencies. Pure automata theory.**

## Architecture

```
Pattern String → [Parser] → AST → [Thompson's] → NFA → [Subset] → DFA → [Hopcroft] → Minimal DFA
                                        ↓
                                  NFA Simulation
                               (captures, anchors)
```

### Four stages:

1. **Parser** — Precedence-climbing parser converts regex strings to AST nodes (alternation < concatenation < quantifier < atom)
2. **Thompson's Construction** — AST → NFA with epsilon transitions. Each operator creates a small fragment (start + end states) that gets wired together
3. **Subset Construction** — NFA → DFA via powerset construction. Each DFA state represents a set of reachable NFA states
4. **Hopcroft Minimization** — Partition refinement to find the smallest equivalent DFA

### Dual execution:

- **DFA path** — Used for simple patterns (no anchors). Pre-compiled, O(n) matching
- **NFA path** — Used for anchored patterns and capture groups. Multi-state simulation avoids exponential backtracking

## Features

| Feature | Example | Description |
|---------|---------|-------------|
| Literals | `abc` | Exact character matching |
| Dot | `.` | Any character (except \n) |
| Alternation | `cat\|dog` | Either branch |
| Kleene star | `a*` | Zero or more |
| Plus | `a+` | One or more |
| Optional | `a?` | Zero or one |
| Groups | `(ab)+` | Capturing groups |
| Classes | `[a-zA-Z]` | Character ranges |
| Negation | `[^0-9]` | Negated classes |
| Shorthands | `\d \w \s` | Digit, word, whitespace |
| Anchors | `^...$` | Start/end of string |
| Repetition | `a{2,5}` | Counted repetition |
| Escapes | `\. \* \\` | Literal special chars |
| Lazy | `a*? a+?` | Non-greedy (parsed, NFA-level) |

## API

```javascript
const { Regex } = require('./regex.js');

// Full match (entire string)
const r = new Regex('[a-z]+@[a-z]+\\.[a-z]+');
r.test('user@example.com');  // true

// Capture groups
const date = new Regex('(\\d{4})-(\\d{2})-(\\d{2})');
date.match('2026-04-06');  // ['2026', '04', '06']

// Search (first match in string)
const digits = new Regex('\\d+');
digits.search('abc 42 def');  // { match: '42', index: 4 }

// Find all non-overlapping matches
digits.findAll('a1b23c456');  // [{ match: '1', ... }, { match: '23', ... }, { match: '456', ... }]

// Replace
const vowels = new Regex('[aeiou]');
vowels.replace('hello', '*', true);  // 'h*ll*'
```

## Why NFA Over Backtracking?

Most regex engines (Perl, Python, JavaScript's built-in) use backtracking, which is simple but vulnerable to exponential blowup on pathological patterns like `a?ⁿaⁿ` matched against `aⁿ`.

This engine uses Thompson's NFA simulation — it tracks all possible states simultaneously, guaranteeing O(nm) time where n is the input length and m is the pattern size. No exponential blowup, ever.

```
Pattern: a?²⁰a²⁰   Input: a²⁰
This engine:  < 1ms  (NFA simulation)
Backtracking: would take ~1 million steps
```

The DFA path goes even further — pre-compiles the NFA into a deterministic automaton for O(n) matching with no per-character branching.

## Tests

```
110 tests | 0 failures
```

Covers: parser, NFA matching, DFA matching, character classes, anchors, repetition, escapes, capturing groups, search/findAll/replace, real-world patterns (email, IP, dates, URLs), edge cases, and performance.

## How It Works (Key Algorithms)

### Thompson's Construction
Each regex operator maps to a small NFA fragment:
- **Literal `a`**: two states connected by an `a`-transition
- **Concatenation `AB`**: wire A's end to B's start with an ε-transition
- **Alternation `A|B`**: new start with ε-transitions to both, both ends wire to new accept
- **Kleene star `A*`**: loop from end back to start, plus bypass from start to accept

### Subset Construction
Build a DFA where each state is a *set* of NFA states. Start with the ε-closure of the NFA start state. For each input symbol, compute the set of reachable NFA states — that's a new DFA state.

### Hopcroft Minimization
Partition DFA states into equivalence classes. Initially: {accepting} and {non-accepting}. Iteratively refine: split any partition whose states disagree on which partition a transition leads to. Fixed point = minimal DFA.

## Files

```
regex.js  — Parser, NFA, DFA, minimization, Regex class
test.js   — 110 tests
README.md — This file
```
