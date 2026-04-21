# HenryDB Next Phase — Architecture Analysis
- created: 2026-04-21
- tags: henrydb, architecture, planning

## Current Architecture Summary (Apr 21)
- **SQL Parser**: Custom hand-written parser (sql.js), full SQL support
- **Storage**: File-backed heaps (page-based), B+tree indexes (both in-memory and file-backed)
- **MVCC**: Version maps with xmin/xmax, PK-based scan dedup, PK-level conflict detection
- **Isolation**: Repeatable Read with SSI (serializable snapshot isolation)
- **Recovery**: FileWAL with checkpoint/truncation for both PersistentDB and TransactionalDB
- **Query Engine**: Interpreted + compiled (adaptive engine), cost-based optimizer, hash/merge/NL joins
- **Wire Protocol**: PostgreSQL-compatible (pg_catalog, etc.)

## Key Fixes Shipped Today
1. MVCC multi-version heap visibility (PK dedup in scan)
2. PK-level write-write conflict detection
3. Division type awareness (DECIMAL/FLOAT columns)
4. Compiled engine expression safety (no more silent-null filters)
5. PersistentDB WAL checkpoint
6. sql-functions.js extraction (-695 LOC from db.js)

## Priorities for Next Phase

### P1: Dead Tuple Vacuum
After repeated UPDATEs, physical heap accumulates dead versions. Current state:
- PK dedup in scan masks the problem (correct results)
- But heap grows unbounded, slowing full scans
- Need: mark dead tuples as reclaimable, periodically compact pages

### P2: More Monolith Extraction
db.js still 9193 LOC. Next extraction candidates:
- Window functions (~400 LOC): _computeWindowFunctions and helpers
- EXPLAIN/plan formatting (~400 LOC): _explain, _formatPlan, etc.
- DDL handlers (~500 LOC): _createTable, _dropTable, _alterTable, etc.
- DML handlers (~300 LOC): _insert, _insertSelect, triggers

### P3: Stress Testing Infrastructure
The Apr 20 stress test suite found 3 critical bugs. Need:
- Automated stress test runner (not just unit tests)
- Concurrent client simulation
- Property-based testing (random SQL generation)

### P4: Buffer Pool Improvements
Current buffer pool is basic. Could add:
- Clock or LRU-K eviction policy
- Dirty page tracking for WAL checkpoint optimization
- Pin counting for concurrent access safety
