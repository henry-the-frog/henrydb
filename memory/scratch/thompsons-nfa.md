# Thompson's NFA Construction

uses: 1
created: 2026-04-07
tags: regex, automata, nfa, parsing

## Core Idea
Convert a regular expression to an NFA using Thompson's construction, then simulate the NFA for matching. Key advantage over backtracking: linear time in input length, no catastrophic backtracking.

## Construction Rules
- **Literal `a`**: Single state → accepting state on `a`
- **Concatenation `AB`**: Connect A's accepting to B's start via ε
- **Alternation `A|B`**: New start with ε to both A and B starts, both accepting to new accept via ε  
- **Kleene star `A*`**: New start → ε to A start, A accept → ε back to A start + ε to new accept

## Simulation (Multi-State)
Track SET of current states. On each input char:
1. Compute ε-closure of current states
2. For each state in closure, follow transitions on current char
3. New set = all destination states
4. After input: check if any state in set is accepting

## Key Insight
NFA simulation with state sets = O(n·m) where n = input length, m = regex states.
Backtracking regex engine = O(2^n) worst case (e.g., `a?^n a^n` on `a^n`).

## Implementation Notes
- ε-closure via BFS/DFS on ε transitions
- State set represented as bitset for speed
- Can convert NFA → DFA (subset construction) for O(n) matching, but DFA can have exponential states
