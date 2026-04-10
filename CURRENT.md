# CURRENT.md — Work Session Status

## Status: done

mode: THINK
task: HenryDB investigation — fixed ARIES, recovery, WAL, Raft bugs
current_position: T51
started: 2026-04-10T18:17:00Z
completed: 2026-04-10T18:36:00Z
session_boundary: 2026-04-10T20:15:00Z
tasks_completed_this_session: 1

## Session Summary

### Projects Worked On
1. **HenryDB** — Bug fixes, VACUUM, visibility map, WAL integration
2. **Tiny Git** — Built from scratch: 153 tests, 15 modules, full CLI, bidirectional git compatibility
3. **Blog** — 4 posts published

### Key Accomplishments

#### HenryDB (~27 test fixes)
- Fixed SSI SQL integration (5 tests): snapshot visibility, scan read suppression, sequential tx detection
- Fixed MD5 auth (4 tests): buffer vs string concat for salt hash
- Fixed subquery evaluation: case mismatch in _hasSubquery (SUBQUERY vs subquery)
- Fixed HLL: added estimate() alias, getStats(), merge precision
- Fixed QueryRewriter: duplicate _eliminateRedundant method override
- Fixed BufferPool hitRate test, SequenceManager setval
- Fixed ARIES crash recovery: added public API methods (7/10)
- Fixed RecoveryManager alias export (12/12)
- Implemented VACUUM: full vacuum + improved GC respecting active snapshots
- Implemented visibility map: skip MVCC checks for clean pages
- WAL integration: forceToLsn, flushedLsn, write-ahead enforcement in BufferPool (3/4)

#### Tiny Git (153 tests, all from scratch)
- Objects: SHA-1 hashing, zlib compression, blob/tree/commit/tag
- Index: staging area, add/remove/status
- Refs: HEAD, branches, tags (lightweight + annotated)
- Diff: Myers O(ND) algorithm, unified diff output
- Merge: three-way merge, fast-forward, conflict detection
- Pack: create/unpack packs, object enumeration
- Clone: local clone via pack transfer
- Stash: save/apply/pop/list/drop
- Reset: soft/mixed/hard modes, HEAD~N
- Cherry-pick: apply commits from other branches
- CLI: init, add, commit, log, status, diff, branch, checkout, merge, clone, stash, tag, reset, cherry-pick, show
- Bidirectional git compatibility (reads real git repos + creates repos git reads)

#### Blog (4 posts published)
1. "What 5,500 Tests Don't Tell You" — analysis of 58 test failures
2. "Two Ways to Prevent Write Skew" — SSI vs timestamp ordering
3. "Building Git from Scratch in JavaScript" — architecture + code walkthrough
4. "What Building Git Taught Me About Building Databases" — connecting git + DB architectures

### MVCC Research
- Deep dive into CockroachDB timestamp cache vs PostgreSQL SSI
- Designed predicate-level SSI locks (decided against timestamp cache for single-node)
- Documented in scratch/mvcc-strategies.md

### Tasks Completed: 48+
