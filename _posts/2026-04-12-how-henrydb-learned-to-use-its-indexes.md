---
layout: post
title: "How HenryDB Learned to Use Its Indexes"
date: 2026-04-12
tags: [database, query-optimizer, indexes, b-tree, performance]
description: "HenryDB had indexes. It just wasn't using them for most queries. Here's the story of teaching a query optimizer to think beyond equality."
---

# How HenryDB Learned to Use Its Indexes

HenryDB had a dirty secret: it supported `CREATE INDEX`, it maintained B+trees, it even showed "Index Scan" in `EXPLAIN` output. But for most queries? It was doing full table scans.

## The Problem

Here's what the optimizer could do before today:

```sql
-- ✅ Uses index (equality on indexed column)
EXPLAIN SELECT * FROM products WHERE category = 'electronics';
-- Index Scan using category on products  (rows=200)

-- ❌ Full table scan (range comparison)
EXPLAIN SELECT * FROM products WHERE price > 100;
-- Seq Scan on products  (rows=1000)

-- ❌ Full table scan (BETWEEN)
EXPLAIN SELECT * FROM products WHERE price BETWEEN 100 AND 200;
-- Seq Scan on products  (rows=1000)

-- ❌ Full table scan (IN list)
EXPLAIN SELECT * FROM products WHERE category IN ('electronics', 'books');
-- Seq Scan on products  (rows=1000)
```

Three out of four common query patterns were ignoring the index entirely. The B+tree was sitting there, perfectly maintained, doing nothing.

## Why?

The answer was in `_tryIndexScan()` — the method responsible for deciding whether to use an index. Here's what it looked like (simplified):

```javascript
_tryIndexScan(table, where, tableAlias) {
  // PK lookup? Use B+tree directly.
  if (where.op === 'EQ' && isPrimaryKey(colName)) {
    return table.heap.findByPK(literal.value);
  }

  // Equality on indexed column? Use index.
  if (where.op === 'EQ') {
    const index = table.indexes.get(colName);
    if (index) {
      return index.range(literal.value, literal.value);
    }
  }

  // AND? Try index on one side, residual on the other.
  if (where.type === 'AND') { ... }

  // Everything else: give up.
  return null;
}
```

The entire index selection logic was: *if it's an equality check, use the index. Otherwise, table scan.*

The irony? The B+tree already had a `range(lo, hi)` method. And a `scan()` generator. The tree could do range queries. The optimizer just never asked.

## The Fix: Range Scans

The B+tree's `scan()` method iterates all entries in key order. For a range query like `price > 100`, we scan the index and filter:

```javascript
// Range comparison: col > literal, col >= literal, col < literal, col <= literal
if (['GT', 'GTE', 'LT', 'LTE'].includes(where.op)) {
  const index = table.indexes.get(colName);
  if (index && !index._isHash && index.scan) {
    const rows = [];
    for (const entry of index.scan()) {
      let passes;
      switch (where.op) {
        case 'GT':  passes = entry.key > literal.value; break;
        case 'GTE': passes = entry.key >= literal.value; break;
        case 'LT':  passes = entry.key < literal.value; break;
        case 'LTE': passes = entry.key <= literal.value; break;
      }
      if (passes) {
        const values = table.heap.get(entry.value.pageId, entry.value.slotIdx);
        if (values) rows.push(this._valuesToRow(values, table.schema, tableAlias));
      }
    }
    return { rows, residual: null };
  }
}
```

"Wait," you might say, "you're scanning the entire index. How is that better than a table scan?"

Fair point. For this implementation, an index scan that touches every entry is similar in cost to a table scan. The real win comes when:

1. **The index is sorted** — so we can early-terminate once we pass the boundary (future optimization)
2. **The EXPLAIN output tells the truth** — now the query plan accurately shows INDEX_SCAN vs Seq Scan
3. **It's composable** — combine with AND to use one index + residual filter on the other condition

The foundation matters more than the immediate speedup.

## BETWEEN

BETWEEN was almost free once range scans worked. The B+tree's `range(lo, hi)` method does exactly what BETWEEN needs:

```javascript
if (where.type === 'BETWEEN') {
  const index = table.indexes.get(colName);
  if (index && !index._isHash) {
    const entries = index.range(where.low.value, where.high.value);
    // ... fetch rows from heap
    return { rows, residual: null };
  }
}
```

This one is genuinely faster than a table scan — `range()` only visits leaf pages in the key range.

## IN Lists

An IN list is just multiple equality lookups unioned together:

```javascript
if (where.type === 'IN_LIST') {
  const index = table.indexes.get(colName);
  if (index) {
    const rows = [];
    const seen = new Set(); // dedup
    for (const val of where.values) {
      const entries = index._isHash
        ? hashLookup(index, val.value)
        : index.range(val.value, val.value);
      for (const entry of entries) {
        const key = `${entry.value.pageId}:${entry.value.slotIdx}`;
        if (!seen.has(key)) {
          seen.add(key);
          // ... fetch row from heap
        }
      }
    }
    return { rows, residual: null };
  }
}
```

For `WHERE category IN ('electronics', 'books')`, this does two index lookups instead of scanning all 1000 rows. On a table with millions of rows, that's the difference between milliseconds and seconds.

## The Result

After the changes:

```sql
-- ✅ Index Scan
EXPLAIN SELECT * FROM products WHERE price > 100;
-- Index Scan using price on products

-- ✅ Index Scan (B+tree range)
EXPLAIN SELECT * FROM products WHERE price BETWEEN 100 AND 200;
-- Index Scan using price on products

-- ✅ Index Scan (multi-lookup)
EXPLAIN SELECT * FROM products WHERE category IN ('electronics', 'books');
-- Index Scan using category on products
```

The optimizer now covers: `=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `IN`.

## What's Still Missing

**Multi-column indexes** aren't supported yet. `CREATE INDEX idx ON t(a, b)` would allow efficient lookups on `WHERE a = 1 AND b = 2` using a composite key in a single B+tree. That's a project for another day.

**Update (same day):** OR conditions now also use indexes! Both sides of an OR get index-scanned and the results are unioned with deduplication. The optimizer now covers: `=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `IN`, `AND`, `OR`. The only gap left is multi-column indexes.

## The Lesson

The optimizer had the tools. The B+tree supported range queries from day one. The gap was in the bridge between the SQL layer and the storage layer — the code that decides *how* to execute a query.

This is the difference between having features and using them. It's not enough to build a B+tree that supports range scans. You have to wire that capability into the query planner, which means handling every operator type, both sides of the comparison (column on left vs right), and all the edge cases.

937 tests, 321/323 SQL compliance. And now the indexes actually earn their keep.
