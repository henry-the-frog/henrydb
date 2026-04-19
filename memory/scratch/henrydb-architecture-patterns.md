# HenryDB Architecture Patterns (2026-04-19)

## Expression Walker Duplication
These functions ALL walk expression trees but each covers DIFFERENT node types:
- `_exprContainsWindow(node)` — checks if expr has window function
- `_exprContainsAggregate(expr)` — checks if expr has aggregate
- `_extractWindowNodes(node)` — extracts window nodes, assigns keys
- `_collectAggregateExprs(expr)` — collects aggregate expr nodes
- `_evalGroupExpr(expr, groupRows, ...)` — evaluates expr in GROUP BY context
- `_evalAggregateExpr(expr, rows)` — evaluates expr in whole-table agg context

### Node types that MUST be handled by all walkers:
- `arith` (left/right)
- `COMPARE` (left/right)
- `cast` (expr)
- `function_call` (args[])
- `case_expr` (whens[condition/result], elseResult)
- `IS_NULL` / `IS_NOT_NULL` (left)
- `NOT` (expr)
- `unary_minus` (operand)
- `aggregate_expr` (leaf for agg, recurse for others)
- `window` (leaf for window, recurse for others)

### Solution: Generic expression walker
```javascript
function walkExpr(node, visitor) {
  if (!node) return visitor.default?.();
  if (visitor[node.type]) return visitor[node.type](node);
  // Generic tree walk
  for (const key of ['left', 'right', 'expr', 'operand']) {
    if (node[key]) walkExpr(node[key], visitor);
  }
  if (node.args) node.args.forEach(a => walkExpr(a, visitor));
  // ... etc for whens, else
}
```

## Aggregate Evaluation Paths
4 independent paths for evaluating aggregates:
1. `_computeAggregates` — whole-table, no GROUP BY
2. `_selectWithGroupBy` — GROUP BY projection
3. `_computeWindowFunctions` — window function context
4. `_collectAggregateExprs` + HAVING pre-compute

Each had to be independently updated to handle:
- Function-wrapped aggregates (COALESCE(SUM(x), 0))
- CAST-wrapped aggregates (CAST(SUM(x) AS FLOAT))
- Arithmetic with aggregates (SUM(x) / COUNT(*))

## Parser Duplication
`parseSelectColumn` (~200 lines) duplicates `parsePrimary`/`parseExpr`:
- Window functions: handled in both
- Aggregates: handled in both
- NOT/EXISTS: handled in both
- CAST: handled in both
- Trailing arithmetic: had to be patched into parseSelectColumn

Partial fix done: shared helpers (parseWindowCall, parseTrailingArithmetic, AGGREGATE_FUNCS).
Full fix needed: parseSelectColumn delegates to parseExpr, handles only alias/star.
