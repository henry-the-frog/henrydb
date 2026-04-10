# tiny-git 🔧

A Git implementation from scratch in JavaScript. No dependencies. 111 tests.

## What It Does

Implements the core Git data structures and algorithms:

- **Content-addressable object store** — SHA-1 hashing, zlib compression, blob/tree/commit/tag objects
- **Index (staging area)** — add, remove, status detection (staged/modified/untracked/deleted)
- **References** — HEAD, branches, tags, detached HEAD
- **Myers diff algorithm** — O(ND) shortest edit script with unified diff output
- **Three-way merge** — merge base detection (BFS), fast-forward, conflict detection with markers
- **Full CLI** — init, add, commit, log, status, diff, branch, checkout, merge

## Compatibility

Repositories created by tiny-git are **fully readable by real git**:

```bash
$ node src/cli.js init
$ echo "Hello" > README.md
$ node src/cli.js add README.md
$ node src/cli.js commit -m "Initial commit"

# Real git can read it:
$ git log --oneline
dc20173 Initial commit

$ git cat-file -p HEAD
tree db78f3594ec0683f5d857ef731df0d860f14f2b2
author User <user@example.com> 1775837299 -0600
committer User <user@example.com> 1775837299 -0600

Initial commit
```

## Tests

```bash
node --test src/*.test.js
# 111 tests, 0 failures
```

Test categories:
- **Object store** (24 tests) — hashing, roundtrip, compression, git hash compatibility
- **Index** (18 tests) — staging, directory recursion, status, tree building
- **Commands** (13 tests) — init/add/commit/log end-to-end
- **Diff** (16 tests) — Myers algorithm, unified format, edge cases
- **Checkout** (10 tests) — branch switching, detached HEAD, nested dirs
- **Merge** (7 tests) — three-way merge, fast-forward, conflicts
- **Stress** (14 tests) — 100 files, 10-level nesting, 1MB files, binary, Unicode
- **Compatibility** (9 tests) — real git reads our objects, trees, diffs, branches, merges

## Architecture

```
src/
  objects.js    — Content-addressable store (SHA-1, zlib, blob/tree/commit/tag)
  index.js      — Staging area (add, remove, status, tree building)
  refs.js       — References (HEAD, branches, tags)
  commands.js   — High-level commands (init, commit, log)
  diff.js       — Myers diff algorithm + unified output
  checkout.js   — Branch switching + working tree management
  merge.js      — Three-way merge + conflict detection
  cli.js        — Command-line interface
```

## Blog Posts

- [Building Git from Scratch in JavaScript](https://henry-the-frog.github.io/2026/04/10/building-git-from-scratch/)
- [What 5,500 Tests Don't Tell You](https://henry-the-frog.github.io/2026/04/10/what-5500-tests-dont-tell-you/)
