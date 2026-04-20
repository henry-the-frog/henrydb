# Hash Join Wiring Plan

## Problem
TWO separate join paths both do O(n*m) nested loop:
1. `_executeJoin` (line 2313) — for table-to-table joins (the MAIN path)
2. `_executeJoinWithRows` (line 1676) — for view/CTE/subquery joins

The main INNER/LEFT path is at line 2430-2447 in `_executeJoin`. It scans `rightTable.heap` per left row.
`_executeJoinWithRows` is only called for materialized right rows (views/CTEs).

## Simplest Fix (No Planner Integration Needed)
Add hash join logic in `_executeJoin` before the INNER/LEFT loop (line 2430):

```javascript
// In _executeJoin, BEFORE the INNER/LEFT nested loop (line ~2430):
// Detect equi-join: join.on = {type: 'COMPARE', op: 'EQ', left: {type: 'column_ref', name: 'a.id'}, right: {type: 'column_ref', name: 'b.aid'}}

if (join.on?.type === 'COMPARE' && join.on.op === 'EQ' &&
    join.on.left?.type === 'column_ref' && join.on.right?.type === 'column_ref') {
  // Materialize right table once
  const rightRows = [];
  for (const { values } of rightTable.heap.scan()) {
    rightRows.push(this._valuesToRow(values, rightTable.schema, rightAlias));
  }
  
  // Resolve which column is left, which is right
  const col1 = join.on.left.name;
  const col2 = join.on.right.name;
  const rightCol = (rightRows[0] && col1 in rightRows[0]) ? col1 : col2;
  const leftCol = rightCol === col1 ? col2 : col1;
  
  // Build hash map on right side
  const hashMap = new Map();
  for (const row of rightRows) {
    const key = row[rightCol];
    if (!hashMap.has(key)) hashMap.set(key, []);
    hashMap.get(key).push(row);
  }
  
  // Probe from left side
  for (const leftRow of leftRows) {
    const key = leftRow[leftCol];
    const matches = hashMap.get(key) || [];
    for (const rightRow of matches) {
      result.push({ ...leftRow, ...rightRow });
    }
    if (matches.length === 0 && join.joinType === 'LEFT') {
      const nullRow = {};
      for (const col of rightTable.schema) nullRow[`${rightAlias}.${col.name}`] = null;
      result.push({ ...leftRow, ...nullRow });
    }
  }
  return result;
}

// Falls through to existing nested loop for non-equi joins
```

## ALSO fix _executeJoinWithRows (line 1676)
Same pattern for view/CTE joins. The ON condition format is the same.

## Expected Impact
- 1K×1K join: 1.8s → ~10ms (180x speedup)
- 10K×1K join: timeout → ~50ms
- TPC-H multi-table joins: 17s → ~100ms

## Complexity
~30 lines of new code. No planner integration needed.
This doesn't use the planner's cost-based selection (hash vs merge vs NL) — it always uses hash for equi-joins. That's fine for now — hash join is almost always optimal for equi-joins.
