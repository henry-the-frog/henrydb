# PostgreSQL First-Updater-Wins Protocol — Research
- created: 2026-04-20
- uses: 0
- tags: henrydb, mvcc, concurrency, research

## PostgreSQL's Approach (EvalPlanQual)

PostgreSQL uses **EvalPlanQual (EPQ)** for UPDATE/DELETE/SELECT FOR UPDATE under READ COMMITTED:

1. Transaction B scans for target rows using its command-level snapshot
2. If a row is locked by uncommitted Transaction A, B **waits** for A to commit/rollback
3. After A commits:
   - PostgreSQL re-reads the **latest committed version** of the row
   - Re-evaluates B's WHERE clause against the new version
   - If still matches: B updates the latest version (not the old one it originally saw)
   - If no longer matches: B skips the row

This prevents lost updates in READ COMMITTED but has subtleties:
- EPQ only rechecks locked rows, doesn't re-scan the table
- Can lead to "missed rows" if newly-inserted rows match WHERE

### Isolation Level Differences
- **READ COMMITTED**: EPQ re-evaluates on latest committed version (per-statement snapshot)
- **REPEATABLE READ**: If the row was modified by a concurrent committed tx → serialization error
- **SERIALIZABLE**: Full SSI with rw-dependency tracking → serialization error

## HenryDB Current State

We're at **Snapshot Isolation** (similar to REPEATABLE READ):
- Each transaction gets a snapshot at BEGIN time
- All reads see the snapshot, not later changes
- Write-write conflicts detected by xmax checks on the same physical row

Our fix today (fall-through to scan) is correct for snapshot isolation:
- T2 finds the version visible in its snapshot and updates it
- But this creates a "lost update" in the classical sense — T1's change is overwritten

### What We'd Need for Full PostgreSQL Behavior

#### Option 1: READ COMMITTED mode (EPQ-style)
1. When UPDATE finds row locked by another tx → wait (or in JS: error/retry)
2. After concurrent tx commits → re-read latest version
3. Re-evaluate WHERE clause against latest version
4. Update the latest version

This requires:
- Per-statement snapshots (not per-transaction)
- Row-level wait/retry on concurrent xmax
- Predicate re-evaluation on the new row version

#### Option 2: REPEATABLE READ / Snapshot Isolation (current)
1. When UPDATE finds a row whose xmax was set by a committed-since-snapshot tx → error
2. "ERROR: could not serialize access due to concurrent update"
3. Application retries

This is simpler and what we should implement next:
- In `_update()` and `_delete()`: when we find a row via scan that has `xmax` set by a
  transaction committed after our snapshot → raise serialization error
- This is the "first-updater-wins" semantics

## Implementation Plan for HenryDB

### Phase 1: Serialization Error on Concurrent Update (REPEATABLE READ)
In `_update()` and `_delete()`, after finding a visible row to modify:
1. Check if the row has been modified by a concurrent tx (xmax set by committed tx not in snapshot)
2. If so: throw "could not serialize access due to concurrent update"
3. This prevents lost updates entirely under REPEATABLE READ

### Phase 2: READ COMMITTED mode (optional, future)
- Add per-statement snapshot refresh
- Implement EPQ re-evaluation
- More complex but more permissive

## Key Insight
Our current fix (fall-through to scan) correctly finds the right row to update.
The missing piece is detecting when that row was already modified by a committed
concurrent transaction and raising an error instead of silently overwriting.
