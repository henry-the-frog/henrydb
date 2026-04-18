# Integration Boundary Testing — Methodology

_Distilled from Apr 17 depth day: 38+ bugs found using this approach._

## Core Principle

Bugs live at integration boundaries — where two independently-built subsystems share a concept but fail to connect. Unit tests per-component pass; integration tests at boundaries find everything significant.

## Bug Patterns (with checklist)

### 1. Fast-Path Bypass
**Every performance optimization is a potential correctness bypass.**
- [ ] Result cache → Does it respect transaction isolation?
- [ ] Index scan → Does it check MVCC visibility?
- [ ] Direct heap access → Does it go through the same filters as full scan?
- [ ] Any "skip WAL" optimization → Does crash recovery still work?

### 2. Write-Path Coverage
**If you enforce an invariant on INSERT, you must enforce it on ALL write paths.**
- [ ] INSERT — constraints checked?
- [ ] UPDATE — constraints checked?
- [ ] UPSERT (ON CONFLICT DO UPDATE) — constraints checked?
- [ ] MERGE (WHEN MATCHED/NOT MATCHED) — constraints checked?
- [ ] FK CASCADE (SET NULL, SET DEFAULT, CASCADE) — constraints checked?
- [ ] ALTER TABLE (backfill) — constraints checked?

### 3. WAL/Logging Completeness
**Any write that doesn't advance the log = data loss after crash.**
- [ ] DDL operations (CREATE/ALTER/DROP) — WAL-logged?
- [ ] VACUUM physical deletes — WAL-logged?
- [ ] Index maintenance — WAL-logged?
- [ ] Checkpoint boundary — does truncation preserve needed records?

### 4. Layer Boundary Wiring
**When N abstraction layers exist, every feature must be wired through ALL layers.**
- [ ] New feature in inner layer → Does wrapper layer persist it? (catalog, WAL)
- [ ] New method in inner layer → Does wrapper expose/delegate it?
- [ ] Recovery path → Does it handle ALL feature types, not just the original ones?

### 5. Shared Resource + Transaction State
**Any shared global resource must be transaction-aware.**
- [ ] Cache → Keyed by transaction/snapshot, or only used outside transactions?
- [ ] Connection pool → Transaction-scoped or leaked?
- [ ] Prepared statements → Bound to transaction lifetime?

### 6. Serialization Rot
**Save/load code rots when new types are added without updating both paths.**
- [ ] New layer/type added → toJSON updated?
- [ ] New layer/type added → fromJSON updated?
- [ ] Round-trip test exists for EVERY serializable type?

## Testing Strategy

Test at the INTERSECTION of two features:
- Feature A alone: works ✅
- Feature B alone: works ✅  
- Feature A + Feature B: **bugs** 🐛

Examples: VACUUM + MVCC (3 bugs), constraint + UPDATE (3 bugs), GROUP BY + window functions (1 bug), index scan + MVCC (2 bugs), serialization + new layer types (2 bugs).

## Application

When starting a depth session on any project:
1. List all "subsystems" or "features"
2. For each pair of features that share a concept, write a test that exercises both simultaneously
3. Focus on the checklist patterns above
4. Track bugs by pattern category — if you find one fast-path bypass, check ALL fast paths
