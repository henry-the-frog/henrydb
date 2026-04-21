# HenryDB MVCC Interception Analysis
- created: 2026-04-20
- uses: 1
- tags: henrydb, mvcc, architecture

## How MVCC Works in HenryDB

### Architecture: Monkey-patching Heap Methods
Instead of building MVCC into the storage layer, `TransactionalDatabase` monkey-patches 4 methods on each table's heap object:

1. **`heap.scan()`** — Filters rows by MVCC visibility (xmin visible + xmax not visible)
2. **`heap.delete()`** — Marks xmax instead of physically deleting (soft delete)
3. **`heap.findByPK()`** — Checks version map for B+tree results, falls back to scan
4. **`heap.get()`** — Checks version map for direct page/slot access

The patching is done in `_installScanInterceptors()` (L701-L907 of transactional-db.js).

### Data Structures
- **Version Maps** (`this._versionMaps`): `Map<tableName, Map<"pageId:slotIdx", {xmin, xmax}>>` 
  - Every row gets an entry when created within a transaction
  - `xmin` = creating tx ID, `xmax` = deleting tx ID (0 if alive, -1 if permanently deleted)
- **Visibility Map** (`this._visibilityMap`): Per-page "all-visible" flags for optimization
  - If page is all-visible, skip MVCC check → direct yield
- **SSI tracking**: `recordRead()` / `recordWrite()` calls on SSIManager for serializable isolation

### Interception Points

| Operation | How MVCC is Applied | Where |
|-----------|-------------------|-------|
| SELECT | `heap.scan()` patched: filters by isVisible(xmin) && !isVisible(xmax) | L714-L777 |
| INSERT | Base Database inserts to heap normally. TransactionalDB adds version map entry post-insert | L422 `_trackNewRows(tx)` |
| UPDATE | Base Database does delete+insert. MVCC intercepts the delete (soft) | DELETE interception L776-L823 |
| DELETE | `heap.delete()` patched: sets ver.xmax = tx.txId instead of physical delete | L776-L823 |
| Commit | MVCC commit + WAL commit + physicalize deletes | L425-L431 |
| Rollback | Undo log reversal + MVCC rollback + WAL abort | L436-L441 |

### Fragility Assessment

**Why it's fragile:**
1. **Order-dependent patching**: `_installScanInterceptors()` must be called after every CREATE TABLE, ALTER TABLE, matview refresh. Missing a call = MVCC bypass.
2. **Closure over `tdb._activeTx`**: Visibility depends on a mutable property on the TransactionalDatabase instance. Concurrent sessions check different `_activeTx` values — but since JS is single-threaded, this works.
3. **Version map keys are strings**: `"pageId:slotIdx"` is fragile if heap pages are compacted or reorganized. VACUUM must handle this carefully.
4. **findByPK fallback to scan**: When B+tree version isn't visible, falls back to full table scan (L843). Performance cliff for point lookups during write-heavy workloads.
5. **No INSERT interception**: Inserts go directly to heap, then version map entry is created after. If a crash happens between insert and version map creation, the row has no MVCC metadata.

**Why it works anyway:**
1. Single-threaded JS = no true concurrency bugs
2. The base Database class is unaware of MVCC — clean separation
3. Undo log for rollback is straightforward (reverse version map changes)
4. WAL + checkpoint ensures durability even if version map is lost (recovery rebuilds)

### Alternative: Proper Visibility Function
The TODO suggests "proper visibility function in HeapFile API." This would mean:
- HeapFile stores xmin/xmax in tuple headers (like PostgreSQL)
- Scan takes a snapshot parameter
- No monkey-patching needed
- Cost: massive refactor of heap storage (page layout, tuple format)

### Recommendation
The interception approach is pragmatic for a learning project. The main risks are:
1. **Missing interception calls** → Add assertion in test suite that all tables have _mvccWrapped flag
2. **findByPK scan fallback** → Acceptable for now, note as TODO for heap-level MVCC
3. **Crash between insert and version map** → Document as known limitation

Don't refactor to heap-level MVCC unless building a production system. The current approach works and keeps the base Database class clean.
