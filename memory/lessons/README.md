# Lessons Index

## Active Lessons

### database-transactions.md
Database transactions, MVCC, WAL, ARIES recovery, and persistence interaction bugs.
Promoted from: henrydb-transactions.md (uses: 7), aries-gap-analysis.md (uses: 2).
Covers: snapshot isolation, SSI, WAL design rules, ARIES three phases, pageLSN, MVCC+persistence boundary bugs, query cache transaction bugs.
Created: 2026-W15 synthesis.

### 2026-04-11-bugs.md
Stress testing finds bugs that unit tests don't. Conv2D gradient normalization, UNION ALL LIMIT, CTE alias resolution, recursive CTE compounding bugs.
Created: 2026-04-11.

### 2026-04-20-stress-testing.md
TPC-H and differential fuzzing lessons: Feature Theater (building capabilities not wired into execution), JS numeric model vs SQL types (10.0 is integer in JS), dual expression parsing paths create inconsistency, differential fuzzing finds bugs that 4000+ unit tests miss.
Created: 2026-04-20.

### 2026-04-19-session-c.md
Evening depth session: variable renames in large files need grep verification (one missed reference broke 71 test files). Non-unique B-tree search() is a footgun — always use range() for equality lookups. Parser keyword conflicts cause subtle case-mismatch bugs. Correlated subquery decorrelation can be done with hash maps without AST rewriting.
Created: 2026-04-19.
