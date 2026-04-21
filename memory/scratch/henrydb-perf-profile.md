# HenryDB Performance Profile (Apr 21)
- created: 2026-04-21
- tags: henrydb, performance, benchmark

## Test Setup
- 1K customers, 5K orders, 15K order_items
- Indexes on: orders.customer_id, order_items.order_id

## Results
| Query Type | Time | Rows | Notes |
|---|---|---|---|
| PK lookup | 4.0ms | 1 | First-query overhead |
| Index scan | 0.5ms | 5 | B-tree range on customer_id |
| Full scan + filter | 4.2ms | 200 | 1K rows → 200 matches |
| 2-way join (1K×5K) | 5.4ms | 50 | Hash join, filtered |
| 3-way join | 3.6ms | 15 | Cascaded hash joins |
| GROUP BY + aggregate | 17.5ms | 5 | 1K×5K join + 5 groups |
| **Correlated subquery** | **259ms** | 20 | **Bottleneck: O(n*m)** |
| Window function | 1.6ms | 5 | ROW_NUMBER, fast |

## Key Finding: Correlated Subquery Performance
The correlated subquery takes 259ms for just 20 outer rows because each execution re-scans 5K orders. PostgreSQL optimizes this with:
1. **Query decorrelation**: Convert `(SELECT COUNT(*) FROM orders WHERE customer_id = c.id)` to `LEFT JOIN (SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id) ...`
2. **Lateral join materialization**: Cache subquery results per distinct outer value

This is the highest-ROI optimization opportunity for HenryDB.
