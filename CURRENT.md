# CURRENT.md
status: session-ended
mode: MAINTAIN
session: B-continued
date: 2026-04-18
started: 2026-04-18T23:00:36Z
ended: 2026-04-19T02:15:00Z

## Session Summary

### HenryDB — Depth Testing Session
**18+ real bugs found and fixed across parser, serialization, MVCC, recovery, persistence:**

**Parser bugs:**
- Escaped quotes dead code (while loop exited before escape check)
- FUNC()+expr in SELECT discarded binary operators after function calls
- Identifier function calls not recognized in SELECT columns
- Unaliased expressions all mapped to same key ("expr"), causing overwrites
- CuckooHashTable hash: `>>> 0 % capacity` parsed as `>>> (0 % cap)` = `>>> 0`

**Serialization/persistence:**
- Views stored as AST but fromJSON expected SQL
- CREATE UNIQUE INDEX not tracked in catalog (startsWith missed 'CREATE UNIQUE')
- UNIQUE flag lost during fromJSON (indexCatalog.columns was array not string)

**MVCC/transactions:**
- ROLLBACK didn't undo physical heap+index changes
- Snapshot-based ROLLBACK needed for chained ops (undo-log failed with stale RIDs)
- Version maps not persisted (deletes lost on reopen after WAL truncation)
- committedTxns load silently failed (nextTxId getter-only, assignment threw)

**Recovery:**
- Recovery destructive heap clear with truncated WAL
- Expression index UPDATE: HOT path didn't track expression column refs

**New features:**
- pg_stat_statements (real query tracking, was phantom feature)
- WAL truncation after checkpoint
- Checkpoint integration in TransactionalDatabase
- CTAS with empty result set (infer schema from AST)
- Recursive CTE JOIN fix (was bypassing JOINs)

**Test count:** 500+ core tests, 0 failures across 45+ test files

### neural-net
- Transformer decoder block (causal masking, cross-attention)
- Beam search, greedy decode, top-K sampling
- MoE tests aligned with existing implementation
- 1337/1337 tests, 0 failures
