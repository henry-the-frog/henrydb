# HenryDB Performance History

## Optimizations Applied (April 10, 2026)

### 1. Group Commit (WAL fsync batching)
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Persistent UPDATE TPS (direct) | 53 | 3,704 | **70x** |
| Persistent TPC-B TPS (wire) | 13 | 53 | **4x** |
| Time per persistent UPDATE | 18.6ms | 0.27ms | 69x faster |

**Root cause:** `fsyncSync()` on every transaction COMMIT took ~18ms on NVMe SSD.  
**Fix:** Batch fsync every 5ms instead of per-commit.

### 2. Scalar Subquery Hoisting
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| `WHERE val > (SELECT AVG(val) FROM t)` | 2,900ms | 8ms | **362x** |
| Queries per second | 0.34 | 125 | 362x |

**Root cause:** Uncorrelated scalar subquery re-evaluated for every outer row.  
**Fix:** Decorrelator detects uncorrelated subqueries and evaluates once, replacing with literal.

### 3. IN Subquery with GROUP BY (bug fix)
| Metric | Before | After |
|--------|--------|-------|
| `WHERE val IN (SELECT MAX(val) FROM t GROUP BY cat)` | 0 results (wrong) | Correct results |

**Root cause:** Decorrelator extracted wrong column (GROUP BY key instead of aggregate).  
**Fix:** Match subquery SELECT column names against result keys.

## Baseline Performance (1000-row table)

| Operation | Speed | Latency |
|-----------|-------|---------|
| SQL Parse (simple) | 93,000 ops/s | 11 µs |
| SQL Parse (complex) | 23,000 ops/s | 44 µs |
| Point lookup (indexed) | 53,000 ops/s | 19 µs |
| INSERT | 25,000 ops/s | 40 µs |
| SELECT * (full scan) | 235 ops/s | 4.2 ms |
| JOIN (500×1000) | 309 ops/s | 3.2 ms |
| GROUP BY (10 groups) | 294 ops/s | 3.4 ms |
| UPDATE (single row) | 756 ops/s | 1.3 ms |

## Wire Protocol Overhead

| Mode | TPS | Notes |
|------|-----|-------|
| In-memory (direct) | ~478 | No wire protocol, no persistence |
| Persistent (direct, batch) | ~3,704 | Group commit, no wire protocol |
| Persistent (wire, batch) | ~53 | pg client over TCP |
| COPY FROM STDIN | ~10,000 rows/s | 4.5x faster than individual INSERT |
| Batched queries (10/call) | 2.4x vs individual | Amortizes TCP round-trip |

## Index Impact

| Lookup Type | Without Index | With Index | Speedup |
|-------------|--------------|------------|---------|
| Point lookup (10K rows) | 21.2 ms | 0.083 ms | **256x** |

## TPC-B ACID Verification

| Test | Result |
|------|--------|
| Sequential (100 txns) | ACID holds ✓ |
| Transactional (5 workers × 20 txns) | ACID holds ✓ |
| Persistent + restart | ACID holds ✓ |
| Write skew (SSI) | Correctly prevented ✓ |
| High contention (5 accounts, 10 workers) | ACID holds, 38 TPS ✓ |
