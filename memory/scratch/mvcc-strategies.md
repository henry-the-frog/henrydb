# MVCC Implementation Strategies — Comparison with HenryDB

uses: 1
created: 2026-04-10
tags: database, mvcc, version-storage, concurrency-control, architecture

## The Four MVCC Design Dimensions (Pavlo framework)

### 1. Version Storage
How/where are old versions kept?

**Append-Only (PostgreSQL)**
- New version is a new physical tuple in the same table
- Old versions link to new via chain pointer
- Dead tuples remain until VACUUM cleans them
- Pro: Write is a simple insert (fast)
- Con: Table bloat, VACUUM overhead, old→new traversal for reads

**Delta Storage (MySQL/InnoDB, Oracle)**
- Main tuple is always the latest version
- Old versions stored as reverse deltas in a separate undo segment
- To reconstruct old version: apply delta chain to current tuple
- Pro: Latest version is always fast to read
- Con: Long-running transactions mean long delta chains

**Time-Travel Storage (HyPer, SAP HANA)**
- Separate time-travel table holds old versions
- Main table always has current version
- Pro: Clean separation, easy GC
- Con: Extra space, cross-table lookup for old versions

**HenryDB's Approach: Append-Only with xmin/xmax**
- Like PostgreSQL: each row has `xmin` (creating tx), `xmax` (deleting tx)
- Version chains stored in-place in version maps (memory)
- No undo segments — old versions are just entries in the Map
- Pro: Simple, works well for in-memory DB
- Con: No persistence for version metadata (must replay from WAL on crash)

### 2. Garbage Collection
When to clean up old versions?

**Tuple-Level GC (PostgreSQL VACUUM)**
- Scan each tuple, check if any active tx can see it
- Remove if xmax < oldest active tx
- Can be background (autovacuum) or manual

**Transaction-Level GC (Oracle/InnoDB)**
- Track which transactions are complete
- Clean undo segments when no tx can reference them
- More efficient for write-heavy workloads

**Epoch-Based GC (HyPer)**
- Assign epochs to transactions
- GC entire epoch when no active tx from that epoch
- Very efficient, batch cleanup

**HenryDB: Minimal GC**
- SSI _cleanupOldInfo removes committedInfo when txId < activeMinTx - 100
- No VACUUM for actual data rows (memory-only DB)
- Version map grows unbounded for long-running workloads
- **Gap:** Would need real GC if running persistent workloads

### 3. Index Management
How do indexes handle multiple versions?

**Logical Pointers (PostgreSQL)**
- Index points to physical location of latest version
- HOT (Heap-Only Tuples) optimization: if update doesn't change indexed columns, new version can be found via heap chain without index update

**Physical Pointers (Oracle/InnoDB)**
- Secondary indexes point to primary key
- Primary key lookup always finds current version
- No index bloat from updates

**HenryDB: Direct Version Lookup**
- No B-tree integration with MVCC currently
- Version maps keyed by `pageId:slotIdx`
- Index lookups go through heap scan with MVCC filtering
- **Gap:** Indexes don't skip invisible rows (no visibility-aware index scan)

### 4. Concurrency Control Protocol
How to detect and resolve conflicts?

**MVTO (Multi-Version Timestamp Ordering)**
- Each tx gets a timestamp at BEGIN
- Reads check timestamp; writes check for WW conflicts
- Used by: HyPer

**MVOCC (Multi-Version Optimistic CC)**
- Read phase: reads from snapshot
- Validation phase: check for conflicts at commit
- Write phase: make changes visible
- Used by: MySQL NDB Cluster, MemSQL

**MV2PL (Multi-Version Two-Phase Locking)**
- Shared/exclusive locks on tuples
- Reads acquire shared locks on latest visible version
- Used by: MySQL/InnoDB (S2PL), PostgreSQL (with SSI extension)

**SSI (Serializable Snapshot Isolation)**
- Read from consistent snapshot
- Track rw-antidependencies
- Abort if dangerous structure detected
- Used by: PostgreSQL 9.1+, **HenryDB**

**HenryDB's Protocol: SSI + MVCC**
- Base: Snapshot Isolation (read from consistent snapshot, first-writer-wins for WW)
- Extension: SSI for SERIALIZABLE level (rw-dependency tracking, dangerous structure detection)
- Isolation levels: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- **Strength:** Correct SSI implementation with proper snapshot visibility
- **Weakness:** Read tracking at row level during scans (can be coarse-grained)

## CockroachDB Serializable Isolation (researched 2026-04-10)

CockroachDB uses a fundamentally different approach than PostgreSQL/HenryDB for serializability:

### Timestamp Ordering (not SSI!)
- Every transaction gets a HLC (Hybrid Logical Clock) timestamp at begin
- Serializability enforced by ensuring the serializability graph is acyclic
- Key rule: **operations can only conflict with EARLIER timestamps**
- If a conflict with a LATER timestamp is detected → abort and restart with new timestamp

### Three Conflict Types (handled differently)

**Write-Read (WR) — MVCC Snapshots**
- Keys store multiple timestamped versions
- Reads return the most recent version with timestamp < operation timestamp
- Result: reads always see a "snapshot" — can't read future values
- Same as PostgreSQL/HenryDB

**Read-Write (RW) — Timestamp Cache**
- Each node has an in-memory timestamp cache: key → most recent read timestamp
- Before writing, check cache: if key was read at a LATER timestamp → abort
- **This is how they detect write skew!** No need for rw-dependency tracking
- Cache stores key RANGES (interval cache) — handles predicate reads/scans
- Size-limited LRU with a "low water mark" (oldest timestamp still in cache)

**Write-Write (WW) — Latest version check**
- Can't write to key that already has a version with a later timestamp
- Simply check MVCC chain: if newer version exists → abort

### Key Differences from HenryDB

| Aspect | HenryDB | CockroachDB |
|--------|---------|-------------|
| Conflict detection | SSI (dependency graph) | Timestamp ordering |
| Write skew | Track rw-dependencies, detect cycles | Timestamp cache aborts stale writes |
| Abort target | Tx with dangerous structure | Always abort the "earlier" tx |
| Distribution | Single node | Distributed (each node has local cache) |
| False positives | Possible (scan-level reads) | Possible (interval cache, LRU eviction) |
| Restart strategy | Client retries | Automatic restart with new timestamp |

### The Timestamp Cache is Brilliant
- Solves the exact problem we had with scan-level false deps
- Instead of tracking which rows each tx read, track which keys have been read recently
- Any write that would create a RW conflict with a later read → abort
- Interval-based: scan over range [a, z] puts the RANGE in the cache, not each key
- This means even phantom-level anomalies are prevented!

### What HenryDB Could Learn
1. **Timestamp cache approach** is simpler than SSI dependency graphs
2. **Interval cache for scans** prevents false positives from individual row tracking
3. **Automatic restart** is better UX than "abort and let client retry"
4. But: SSI is more nuanced and can allow more concurrency in some cases (both approaches have tradeoffs)

### Parallel Commits
CockroachDB's distributed commit protocol:
1. Write intents (provisional MVCC values with tx record pointer) via Raft
2. Set tx record to STAGING
3. Verify all write intents were replicated
4. Respond to client (committed!)
5. Async cleanup: move tx record to COMMITTED, resolve intents to regular MVCC values
6. Write intents can be resolved by ANY future reader (not just the committing tx)

This is more sophisticated than HenryDB's 2PC — pipelining writes with validation makes commits much faster for distributed txns.

1. **HenryDB is closest to PostgreSQL** in architecture: append-only versions, xmin/xmax metadata, SSI for serializability, VACUUM-style GC (minimal)
2. **The main gap is persistence:** PostgreSQL writes version metadata to heap pages; HenryDB keeps it in memory maps and rebuilds from WAL
3. **Index-MVCC integration is missing:** Real databases have visibility-map-aware index scans; HenryDB does full scan + MVCC filter
4. **GC is the next frontier:** For any serious workload, the version map will grow. Need epoch-based or VACUUM-based cleanup
5. **Delta storage would be better for updates:** If HenryDB evolves toward persistent storage, InnoDB-style undo segments would be more space-efficient than append-only

## What I'd Do Differently

If starting HenryDB's MVCC over:
1. **Separate version storage** from heap (time-travel approach) — cleaner architecture
2. **Undo segments** for old versions instead of full-tuple copies
3. **Epoch-based GC** — simpler than VACUUM, works well for in-memory
4. **Predicate-level SSI tracking** — avoid false positives from scan-level reads (the bug we just fixed!)
5. **Visibility map** — bitmap of pages where all tuples are visible to all (skip MVCC check for known-all-visible pages)
