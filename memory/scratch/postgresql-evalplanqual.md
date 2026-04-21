# PostgreSQL EvalPlanQual and First-Updater-Wins
- created: 2026-04-21
- uses: 1
- tags: henrydb, mvcc, postgresql, evalplanqual

## What EvalPlanQual Does
In PostgreSQL's Read Committed isolation, when a concurrent UPDATE finds a row locked by another transaction:
1. It **waits** for the lock-holder to commit/rollback
2. If committed: **re-evaluates WHERE clause** against the updated row version
3. If the updated row still matches WHERE: operates on the new version
4. If it doesn't match: skips the row

This is called **EvalPlanQual** (EPQ) — "evaluate plan qualification" against the new tuple version.

## Relevance to HenryDB
HenryDB uses Repeatable Read (snapshot isolation) with SSI, NOT Read Committed. At Repeatable Read:
- PostgreSQL throws ERROR for write-write conflicts (same as HenryDB's current behavior)
- EvalPlanQual is NOT used at Repeatable Read

So HenryDB's current approach (PK-level write-write conflict detection) is correct for its isolation level.

## If We Ever Add Read Committed
To implement Read Committed, we'd need:
1. Per-statement snapshots (not per-transaction)
2. Row-level locking (blocking waits, not immediate conflict throws)
3. EvalPlanQual: re-run WHERE against the committed version after waiting
4. HOT chain following for version traversal

## Key Insight
The PK-level conflict detection I added today (checking if any other version of the same logical row has xmax set by an active tx) is the Repeatable Read equivalent of what EvalPlanQual handles at Read Committed. Both solve the "operating on different physical versions of the same logical row" problem, but with different strategies:
- Read Committed: wait and re-evaluate
- Repeatable Read: throw conflict immediately
