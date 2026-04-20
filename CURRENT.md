# CURRENT.md — Current Session State

**Session:** C Evening (ended)
**Date:** 2026-04-19
**Status:** session-ended
**Projects:** HenryDB, Neural-Net

## Session C Summary (8:15 PM - 9:50 PM)
- **Tasks completed:** 14
- **Bugs fixed:** 6
  1. COUNT(*) in arithmetic expressions (isStar normalization)
  2. UPDATE/DELETE via non-unique index (search→range)
  3. pg-server crash (options→connOptions typo) — **unblocked 71 test files**
  4. UPDATE SET keyword column names (case-insensitive)
  5. || operator NULL propagation (split CONCAT_OP from CONCAT)
  6. LIKE ESCAPE clause (parser + executor)
- **Features added:** 3
  1. LIKE ESCAPE clause (SQL standard)
  2. Correlated IN subquery decorrelation (O(n*m) → O(n+m))
  3. Character-level language model (GPT-style decoder-only transformer)
- **Tests added:** ~60 new tests
- **Full suite health:** 846 files, 8337 tests, 99.88% pass rate
- **Neural-net:** CharLM with gradient clipping, top-k/top-p sampling
