# MVCC State Persistence Bug Chain

## The Problem
After checkpoint + WAL truncation + reopen, DELETE operations were lost. Deleted rows reappeared.

## Root Cause Chain (3 bugs, one masking the other)

### Bug 1: Version maps not persisted
- `_saveMvccState()` only saved `nextTxId`, not the version maps (xmin/xmax per row)
- On recovery, version maps rebuilt from heap scan → all physical rows → all visible
- MVCC deletes are logical (set xmax), not physical — heap still has deleted rows

### Bug 2: committedTxns not persisted
- Even after fixing Bug 1, deletes still appeared because `committedTxns` was empty on reload
- The MVCC scan checks if `xmax`'s transaction is in `committedTxns` before treating it as deleted
- Without `committedTxns`, `xmax: 11` was ignored (tx 11 not known to be committed)

### Bug 3: Silent getter-only property failure
- `mvcc.nextTxId = state.nextTxId` threw because `nextTxId` is a getter-only property
- The `try { } catch { /* best effort */ }` silently swallowed the error
- The `committedTxns` loading code was AFTER `nextTxId` in the same try block
- Fix: use `mvcc._nextTx` (underlying property)

## Pattern: Silent Try-Catch + Property-Order Dependencies
When a try-catch swallows errors, and you have multiple assignments in sequence, a failure in assignment N prevents all assignments N+1, N+2, etc. If any property is getter-only, the assignment silently fails.

**Lesson**: In recovery/serialization code, either:
1. Separate each load into its own try-catch
2. Don't use "best effort" catch for properties that MUST succeed
3. Test getters/setters explicitly in persistence tests

## Related: WAL Truncation + Destructive Recovery
The recovery function cleared all heap data and replayed from WAL. After truncation, no WAL records to replay → data lost. Fix: detect truncated WAL (CHECKPOINT record but no data records before it) and skip destructive clear.

uses: 1
created: 2026-04-18
