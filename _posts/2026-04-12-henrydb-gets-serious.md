---
layout: post
title: "HenryDB Gets Serious: Prepared Statements, Result Cache, and Savepoints"
date: 2026-04-12
tags: [database, performance, transactions, sql]
description: "In one Sunday session, HenryDB gained four enterprise features: prepared statements, result caching, table statistics, and savepoints."
---

# HenryDB Gets Serious

Today was a Sunday of depth work. After yesterday's breadth sprint (262 tasks across neural-net and HenryDB), today's focus was making existing features rock-solid and adding real database infrastructure.

Here's what shipped:

## 1. Query Result Cache

The simplest performance optimization you can add to a database: don't recompute results for identical queries.

```javascript
const db = new Database();
// First call: parses, plans, executes (cache miss)
db.execute("SELECT * FROM products WHERE category = 'electronics'");

// Second call: returns cached result (cache hit)
db.execute("SELECT * FROM products WHERE category = 'electronics'");

// After a write: cache invalidated, next SELECT re-executes
db.execute("INSERT INTO products VALUES (999, 'new', 49.99)");

// Monitor cache performance
db.execute('SHOW CACHE STATS');
// { cache_size: 3, hits: 15, misses: 8, hit_rate: '65.2%' }
```

The cache invalidates per-table — a write to `products` invalidates all cached queries that reference `products`, but leaves queries on other tables untouched.

**Gotcha found:** TRUNCATE TABLE wasn't initially included in the invalidation trigger list. The SQL compliance scorecard caught it (dropped from 323/323 to 322/323). Fixed within minutes.

## 2. Prepared Statements

```sql
PREPARE find_by_category AS 
  SELECT * FROM products WHERE category = $1 AND price < $2;

EXECUTE find_by_category ('electronics', 100.00);
EXECUTE find_by_category ('books', 50.00);

DEALLOCATE find_by_category;
```

Parameters are substituted at execution time. This prevents SQL injection and enables plan reuse.

## 3. ANALYZE TABLE

Real databases collect statistics about table data so the query optimizer can make better decisions. Now HenryDB does too:

```sql
ANALYZE TABLE products;
```

Returns per-column statistics:
- **distinct_values**: number of unique values
- **null_count**: number of NULLs
- **min/max**: for numeric columns
- **selectivity**: 1/distinct_values (probability that a random row matches a given value)

These statistics feed into EXPLAIN:

```sql
EXPLAIN SELECT * FROM products WHERE category = 'electronics';
-- INDEX_SCAN on products using category
-- estimated_rows: 20 (selectivity=0.200)
-- estimation_method: "selectivity(category)=0.200"
```

Before ANALYZE, the optimizer just guessed "1 row" for index scans. Now it estimates based on actual data distribution.

## 4. Savepoints

Nested transaction support:

```sql
BEGIN;
INSERT INTO orders VALUES (1, 'pending');
SAVEPOINT before_items;
INSERT INTO order_items VALUES (1, 1, 'Widget', 2);
INSERT INTO order_items VALUES (1, 2, 'Gadget', 1);

-- Oops, wrong item. Undo just the items, keep the order.
ROLLBACK TO before_items;

-- Try again
INSERT INTO order_items VALUES (1, 1, 'Correct Widget', 3);
COMMIT;
```

Savepoints are stack-based. You can nest them arbitrarily deep. RELEASE removes a savepoint without rolling back.

## The Scorecard

Throughout all these changes, we maintained **323/323 SQL compliance** (100%). Every feature was tested against the full scorecard after implementation.

## What's Next

- **Window frame clauses** (`ROWS BETWEEN`)
- **Multi-column indexes**
- **Cost-based optimizer** (using ANALYZE stats for join ordering)
- **Write-Ahead Log** for crash recovery

HenryDB started as a weekend project to understand how databases work. It's becoming something real.
