# ARIES Recovery: HenryDB Gap Analysis

## What ARIES Requires (That HenryDB Now Partially Has)

### Core Concept: LSN is King
- **pageLSN**: Every page stores the LSN of the last update applied to it
  - HenryDB: ❌ Pages don't have pageLSNs. We use `lastAppliedLSN` per DiskManager instead.
  - Impact: Can't do per-page redo decisions. Recovery is all-or-nothing per table.
  
- **flushedLSN**: Max LSN flushed to disk
  - HenryDB: ✅ We track `_flushedLsn` on the WAL.
  
- **recLSN**: First log record that dirtied a page
  - HenryDB: ❌ Not tracked. This determines where redo starts in ARIES.

### Three Phases
1. **Analysis**: Scan from last checkpoint, rebuild ATT + DPT
   - HenryDB: Partial — we scan WAL for committed/aborted txns but don't track ATT or DPT.
   
2. **Redo**: Replay from min(recLSN in DPT), skip if pageLSN >= record LSN
   - HenryDB: ❌ We don't compare per-page LSNs. We either replay everything or nothing.
   - This is why Bug #3 (checkpoint trap) existed — no per-page LSN tracking.
   
3. **Undo**: Walk backward through loser txns, write CLRs
   - HenryDB: ❌ No undo phase. We simply exclude uncommitted txns from redo.
   - This works for FORCE (all committed pages on disk at commit) but not NO-FORCE.
   - Currently safe because PersistentDatabase auto-commits each DML.

### Compensation Log Records (CLRs)
- ARIES logs every undo action during abort/recovery
- CLRs have `undoNext` pointer to skip already-undone work on re-crash
- HenryDB: ❌ Not implemented. Abort just skips records. Safe for now because no explicit transactions.

### Fuzzy Checkpoints
- ARIES allows txns to continue during checkpoint, records ATT + DPT
- HenryDB: ❌ No fuzzy checkpoint. `flush()` + `checkpoint()` are blocking.

## What Today's Fixes Actually Did (in ARIES Terms)

1. **invalidateAll()** = Ensuring buffer pool cache coherence during redo. ARIES handles this via pageLSN comparison — if pageLSN < record LSN, the page needs redo regardless of cache state.

2. **lastAppliedLSN persisted** = Crude approximation of ARIES pageLSN tracking. Instead of per-page LSNs, we track one LSN per table. This means recovery is O(WAL) not O(dirty pages).

3. **Incremental vs full redo** = ARIES's redo phase naturally handles this via pageLSN. If page was already written to disk (pageLSN >= record LSN), skip. HenryDB's approach (full redo when lastAppliedLSN=0) is a rough approximation.

## Next Steps (if going deeper)
1. Add pageLSN to page headers — enables per-page redo decisions
2. Dirty Page Table in checkpoint — know exactly which pages need redo
3. CLRs for abort — needed before supporting multi-statement transactions
4. Fuzzy checkpoints — needed for concurrent workloads

## Key Insight
HenryDB's current approach works for single-statement auto-commit transactions. The bugs we found (and fixed) were at the boundary between "toy recovery" and "real recovery." Adding pageLSNs would be the single biggest correctness improvement — it eliminates the need for the full-vs-incremental heuristic entirely.
