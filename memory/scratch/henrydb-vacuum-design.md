# HenryDB Dead Tuple Vacuum Design
- created: 2026-04-21
- tags: henrydb, vacuum, mvcc, design

## Problem
After N auto-commit UPDATEs on the same row:
- N+1 physical heap rows (1 live, N dead)
- N+1 version map entries
- Full scan reads all N+1 rows (PK dedup filters, but IO cost remains)

## Vacuum Algorithm (Phase 1 — Simple)

### Dead Tuple Identification
A version map entry `{xmin, xmax}` is dead if:
1. `xmin` committed (created by a finished transaction)
2. `xmax !== 0` AND `xmax` committed (deleted/overwritten by a finished transaction)
3. No active transaction's snapshot can see the row
   - Simplified: `xmax < oldestActiveSnapshotXmin` (all active txs started after this version was deleted)
   - If no active transactions: ALL entries with committed xmin + xmax are dead

### Cleanup Steps
1. Build list of dead version map keys
2. Remove dead entries from version map
3. Mark heap slots as deleted (set values to null or use a tombstone)
4. Track freed slot count for statistics

### Phase 1: Version Map Cleanup Only
- Clean version maps, zero heap slot data
- Don't compact heap pages (leave gaps)
- Fast, simple, gets 90% of the benefit

### Phase 2: Heap Compaction (Future)
- Move live tuples to fill gaps in pages
- Update index entries to point to new locations
- Requires more coordination, save for later

### When to Run
- After checkpoint (already flushing state)
- Periodically (every N transactions or on demand)
- On explicit `VACUUM` SQL command

### API
```javascript
db.vacuum()                    // Vacuum all tables
db.vacuum('tableName')         // Vacuum specific table
db.execute('VACUUM')           // SQL command
db.execute('VACUUM t')         // SQL for specific table
```

## Metrics
For a table with 1 row updated 100 times:
- Before vacuum: 101 version map entries, 101 physical rows
- After vacuum: 1 version map entry, 1 physical row (rest zeroed)
- Space savings: ~99%
