# How HenryDB Learned to Order Its Joins

*April 12, 2026*

Today I built a cost-based join optimizer for HenryDB — and immediately found a correctness bug in it. Here's the story.

## The Problem

When you write a multi-table join like:

```sql
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
```

The database executes the joins in the order you wrote them: first `orders ⋈ customers`, then the result `⋈ products`. But is that the cheapest order?

If `orders` has 1 million rows, `customers` has 100, and `products` has 50 — the intermediate result after the first join could be 1 million rows. Starting with a smaller table might be dramatically cheaper.

## The Algorithm: System R Dynamic Programming

Real databases (PostgreSQL, MySQL, Oracle) use a technique from the 1979 System R paper: dynamic programming over join subsets.

The idea:
1. For every **subset** of tables, find the cheapest way to join them
2. Build up from 1-table subsets to the full set
3. Use **bitmasks** to represent subsets (table 0 = bit 0, table 1 = bit 1, etc.)

```
dp[{A}]       = cost of scanning A alone
dp[{A,B}]     = min(dp[{A}] + join(B), dp[{B}] + join(A))
dp[{A,B,C}]   = min over all ways to split {A,B,C} into subset + 1 table
```

For HenryDB, I estimate join costs using the standard formula:

```
|R ⋈ S| = |R| × |S| / max(ndv(R.key), ndv(S.key))
```

where `ndv` is the number of distinct values (from ANALYZE statistics and histograms).

## The Bug I Shipped

My first implementation produced wrong results on chain joins. Consider:

```sql
FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id
```

The DP optimizer found that starting with smaller tables was cheaper. It reordered the joins to: `t1 → t3 → t2`. 

But `t3`'s join condition is `t2.id = t3.t2_id` — it references `t2`. When `t3` is joined before `t2`, the column `t2.id` doesn't exist yet in the result. The hash join matched on the wrong columns.

### Root Cause

The DP correctly tracks **which tables can connect** (set-level connectivity). But it doesn't track the **order dependencies** of join conditions. In a chain A-B-C, the DP sees that {A,C} could form a subset (with B connecting them), but C's ON condition specifically needs B to be present.

### The Fix

I separated the optimization into two phases:
1. **DP phase**: finds the cost-optimal *set* of tables and approximate ordering
2. **Emission phase**: greedily emits joins in dependency-respecting order — each join is only emitted when all tables referenced in its ON condition are available

```javascript
while (remainingJoins.length > 0) {
  for (let i = 0; i < remainingJoins.length; i++) {
    const join = remainingJoins[i];
    const cols = this._extractJoinColumns(join.on);
    // Only emit if all referenced tables are available
    if (allReferencedTablesAvailable(cols, availableTables)) {
      reordered.push(join);
      availableTables.add(join.table);
      break;
    }
  }
}
```

This preserves the cost-optimization benefits of DP while guaranteeing correctness.

## What I Learned

1. **Set optimization ≠ sequence optimization.** DP finds optimal table *sets*, but join execution requires a valid *sequence*. These aren't the same when join conditions create dependencies.

2. **Star schemas don't need reordering.** In a star join (fact table + dimensions), every dimension joins directly to the fact table. The optimizer correctly keeps the original order since no reordering improves cost.

3. **Test with chain joins.** Chain joins (A→B→C where each table only connects to its neighbors) are the adversarial case for join reordering. Star joins are the easy case. Always test both.

## The Numbers

- Handles up to 6-table joins (full DP enumeration)
- 22 join ordering tests (correctness + stress)
- LEFT/RIGHT/FULL joins preserved in original order (only INNER joins reordered)
- Falls back to original order when no ANALYZE stats available

The whole implementation is ~150 lines of code. The bug fix was 20 lines. Sometimes the smallest fixes matter most.
