# Buffer Pool Research Notes

## Context
HenryDB currently keeps all pages in memory (HeapFile.pages array). For large datasets, this won't work — need a buffer pool manager that evicts pages to disk.

## Key Concepts (from CMU 15-445)

### Buffer Pool Architecture
- Fixed-size pool of "frames" in memory, each holding one disk page
- Frame metadata: page_id, dirty_bit, pin_count
- Pages loaded from disk on demand, evicted when pool is full
- Dirty pages must be written back before eviction

### Replacement Policies
1. **LRU** — Evict page with oldest last-access timestamp
   - Hash map + doubly-linked list for O(1) access/eviction
   - Vulnerable to sequential flooding (large scans evict hot pages)
   
2. **Clock (Second Chance)** — Approximates LRU with lower overhead
   - Circular buffer + reference bit per page + clock hand
   - On eviction: sweep, clear ref=1 bits, evict first ref=0 page
   - PostgreSQL uses this (clock-sweep with usage_count capped at 5)
   - Better for multi-threaded environments

3. **LRU-K** — Track Kth-to-last access, not just last access
   - Better handles sequential flooding
   - Higher implementation complexity

### Relevance to HenryDB
- Current HeapFile: all pages in memory (fine for learning, bad for production)
- BTreeTable: also keeps everything in memory (B+tree nodes)
- Future direction: add BufferPoolManager between storage engines and disk
- Implementation order: Clock policy (simpler, PostgreSQL-like) > LRU > LRU-K
- Pin semantics: pages being actively used can't be evicted
- Dirty page write-back: integrate with existing WAL for crash recovery

### Design Sketch
```
BufferPoolManager {
  pool: ArrayBuffer(PAGE_SIZE * numFrames)  // Fixed memory region
  pageTable: Map<pageId, frameId>           // O(1) lookup
  frames: Array<{ pageId, dirty, pinCount, refBit }>
  clockHand: number                          // For clock sweep
  disk: DiskManager                          // Reads/writes pages to file
  
  fetchPage(pageId) → Page                  // Pin + load if not in pool
  unpinPage(pageId, isDirty)                // Decrement pin, mark dirty
  flushPage(pageId)                         // Write dirty page to disk
  evictPage() → frameId                    // Clock sweep to find victim
}
```

This would be a major architectural upgrade — probably 2-3 sessions of work.
