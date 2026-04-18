# Bug Pattern Analysis — 2026-04-17

uses: 1
created: 2026-04-17
tags: henrydb, architecture, bug-patterns

## Session B: 14 Bugs, 5 Categories

### Category 1: Layer Boundary Bugs (4 bugs)
**What:** DDL operations cross TransactionalDB → Database → FileWAL layers but wiring was incomplete.
**Root cause:** Each layer assumed the layer above handled DDL persistence.
**Fix pattern:** Patch methods across layers (logDDL proxy).
**Better fix:** Single DDLPersistenceManager that owns all DDL state transitions.

### Category 2: Recovery Model Gaps (3 bugs)
**What:** Recovery designed for DML-only. DDL added later without updating recovery model.
**Root cause:** "catalog = truth" assumption fails when catalog is stale after crash.
**Fix pattern:** Added DDL replay phase before per-heap recovery.
**Better fix:** Full ARIES-style recovery (Analysis → Redo → Undo).

### Category 3: Non-Atomic Checkpoint (3 bugs)
**What:** Checkpoint leaves inconsistent state between steps.
**Root cause:** Multi-step checkpoint (flush → write marker → truncate → reset) isn't atomic.
**Fix pattern:** Individual fixes for each state inconsistency.
**Better fix:** Single atomic checkpoint method that handles all state transitions.

### Category 4: Parser Context Blindness (2 bugs)
**What:** Parser doesn't support all expression types in all SQL contexts.
**Root cause:** Incremental parser development without cross-context testing.
**Fix pattern:** Added parsePrimary handler for (SELECT ...).
**Better fix:** Expression parser integration tests covering every context.

### Category 5: Incomplete Features (2 bugs)
**What:** Features work in-memory but lack persistence or constraint enforcement.
**Root cause:** No feature completeness checklist.
**Fix pattern:** Added missing persistence/enforcement.
**Better fix:** Checklist: in-memory ✓, WAL logged ✓, catalog persisted ✓, crash recovery ✓, constraints enforced ✓

## Meta-Insight

The highest-ROI investment for HenryDB quality is a **DDL integration test suite** that tests every DDL operation through the full lifecycle:
1. Execute DDL in memory
2. Verify in-memory effect
3. Close and reopen (clean)
4. Verify persistence
5. Simulate crash and reopen (stale catalog)
6. Verify crash recovery
7. Test concurrent DDL + DML

Running this for every DDL type (CREATE/ALTER/DROP for TABLE/INDEX/VIEW/TRIGGER) would catch Categories 1, 2, 3, and 5 — 12 of 14 bugs.
